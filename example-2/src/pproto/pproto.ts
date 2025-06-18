import ReconnectingWebSocket from "reconnecting-websocket";
import { v4 as uuid } from "uuid";

export type PprotoStatus = "connected" | "disconnected";

export const CLOSE_CONNECTION_COMMAND = "e71921fd-e5b3-4f9b-8be7-283e8bb2a531";

export interface CloseConnectionCommand {
  group: number;
  code: string;
  description: string;
}

export class PprotoError extends Error {
  constructor(
    public readonly group: number,
    public readonly code: string,
    public readonly description: string
  ) {
    super(
      `PprotoError(group: ${group}, code: ${code}, description: ${description})`
    );
  }
}

export class TimeoutError extends Error {
  constructor() {
    super("TimeoutError");
  }
}

export type PprotoListener<T, R> = (data: T) => R;

export interface PprotoSubscription {
  unsubscribe(): void;
}

export class PprotoConnection {
  private readonly ws: ReconnectingWebSocket;
  private readonly commands: Record<string, Resolvable<any>> = {};
  private readonly listeners: Record<string, Set<PprotoListener<any, void>>> =
    {};
  private readonly handlers: Record<string, PprotoListener<any, any>> = {};

  private _status: PprotoStatus = "disconnected";
  get status() {
    return this._status;
  }

  constructor(private readonly url: string) {
    this.ws = new ReconnectingWebSocket(url);
    this.ws.onmessage = (e) => this.onMessage(e);

    this.ws.onopen = () => {
      this._status = "connected";
      this.notifyListeners("connected", this);
    };

    this.ws.onclose = () => {
      this._status = "disconnected";
      this.notifyListeners("disconnected", this);
    };
  }

  async sendCommand<T, R>(
    command: string,
    content: T,
    timeout?: number
  ): Promise<R> {
    return new Promise((resolve, reject) => {
      const max_time_life =
        timeout !== undefined
          ? Math.round(new Date().getTime() / 1000 + timeout)
          : undefined;

      const message: ProtocolMessage = {
        id: uuid(),
        command,
        content,
        web_flags: {
          type: "command",
          exec_status: "unknown",
          priority: "normal",
          content_format: "json",
        },
        max_time_life,
        tags: [],
      };
      this.ws.send(JSON.stringify(message));
      this.commands[message.id] = { resolve, reject };

      if (timeout !== undefined) {
        setTimeout(() => {
          reject(new TimeoutError());
          delete this.commands[message.id];
        }, timeout * 1000);
      }
    });
  }

  onEvent(
    type: "connected" | "disconnected",
    listener: PprotoListener<PprotoConnection, void>
  ): PprotoSubscription;

  onEvent<T>(
    type: string,
    listener: PprotoListener<T, void>
  ): PprotoSubscription;

  onEvent(
    type: string,
    listener: PprotoListener<any, void>
  ): PprotoSubscription {
    let listeners = this.listeners[type];
    if (!listeners) {
      listeners = new Set();
      this.listeners[type] = listeners;
    }
    listeners.add(listener);

    return {
      unsubscribe: () => listeners?.delete(listener),
    };
  }

  commandHandler<T, R>(type: string, handler: PprotoListener<T, Promise<R>>) {
    this.handlers[type] = handler;
  }

  async close(command?: CloseConnectionCommand) {
    command = command ?? {
      group: 0,
      code: "",
      description: "",
    };
    this.sendCommand(CLOSE_CONNECTION_COMMAND, command);
    return this.ws.close();
  }

  private onMessage(event: MessageEvent) {
    const message: ProtocolMessage = JSON.parse(event.data);

    switch (message.web_flags.type) {
      case "answer": {
        const command = this.commands[message.id];
        delete this.commands[message.id];

        switch (message.web_flags.exec_status) {
          case "success": {
            command?.resolve(message.content);
            break;
          }

          case "error":
          case "failed": {
            const error: ErrorContent = message.content;
            command?.reject(
              new PprotoError(error.group, error.code, error.description)
            );
          }
        }
        break;
      }

      case "event": {
        this.notifyListeners(message.command, message.content);
        break;
      }

      case "command": {
        const handler = this.handlers[message.command];
        if (handler) {
          (async () => {
            try {
              const answer = await handler(message.content);
              const response: ProtocolMessage = {
                id: message.id,
                command: message.command,
                content: answer,
                webFlags: {
                  type: "answer",
                  execStatus: "success",
                  priority: "normal",
                  contentFormat: "json",
                },
                tags: [],
              };
              this.ws.send(JSON.stringify(response));
            } catch (e) {
              const response: ProtocolMessage = {
                id: message.id,
                command: message.command,
                content: null,
                webFlags: {
                  type: "answer",
                  execStatus: "failed",
                  priority: "normal",
                  contentFormat: "json",
                },
                tags: [],
              };
              this.ws.send(JSON.stringify(response));
            }
          })();
        }
      }
    }
  }

  private notifyListeners(type: string, data: any) {
    const listeners = this.listeners[type];
    if (listeners) {
      for (const listener of listeners) {
        listener(data);
      }
    }
  }
}

export interface ProtocolMessage {
  id: string;
  command: string;
  content: any;
  web_flags: {
    type: "command" | "answer" | "event";
    exec_status: "unknown" | "success" | "failed" | "error";
    priority: "normal";
    content_format: "json";
  };
  max_time_life?: number;
  tags: number[];
}

export interface ErrorContent {
  group: number;
  code: string;
  description: string;
}

interface Resolvable<T> {
  resolve(data: T): void;
  reject(error: Error): void;
}
