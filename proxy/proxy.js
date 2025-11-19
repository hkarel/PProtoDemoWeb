const net = require("net");
const WebSocket = require("ws");
const { v4: uuid } = require("uuid");

const ORIGIN_PORT = process.env["ORIGIN_PORT"] || 28052;
const ORIGIN_HOST = process.env["ORIGIN_HOST"] || "localhost";
const PROXY_PORT = process.env["PROXY_PORT"] || 9000;
const PROXY_HOST = process.env["PROXY_HOST"] || "localhost";
const SOCKET_TIMEOUT = Number.parseInt(process.env["SOCKET_TIMEOUT"], 10) || 1000;

console.log("--------------------------");
console.log(`ORIGIN_PORT: ${ORIGIN_PORT}`);
console.log(`ORIGIN_HOST: ${ORIGIN_HOST}`);
console.log(`PROXY_PORT:  ${PROXY_PORT}`);
console.log(`PROXY_HOST:  ${PROXY_HOST}`);
console.log("--------------------------");

const PROTOCOL_SIGNATURE = [
  0xfe, 0xa6, 0xb9, 0x58, 0xda, 0xfb, 0x4f, 0x5c, 0xb6, 0x20, 0xfe, 0x0a, 0xaf,
  0xbd, 0x47, 0xe2,
];

const decoder = new TextDecoder();
const encoder = new TextEncoder();

function main() {
  const wsServer = new WebSocket.Server({ port: PROXY_PORT, host: PROXY_HOST });

  wsServer.on("connection", (wsClient) => {
    const connectionId = randomId();
    console.log(`[${connectionId}] Client connected`);
    wsClient.pause();

    function disconnect() {
      tcpSocket.destroy();
      wsClient.close();
    }

    const tcpSocket = net.connect(
      {
        port: ORIGIN_PORT,
        host: ORIGIN_HOST,
        timeout: SOCKET_TIMEOUT,
      },
      async () => {
        console.log(`[${connectionId}] Server connected`);

        try {
          await validateSignature(connectionId, tcpSocket);
          await checkCompatibility(connectionId, tcpSocket);
        } catch (e) {
          console.error(
            `[${connectionId}] Connection error, disconnecting. ${e}`
          );
          disconnect();
          return;
        }

        wsClient.on("message", (messageBuffer) => {
          try {
            const message = decoder.decode(messageBuffer);
            console.log(
              `[${connectionId}] Message from client`,
              trimMessage(message)
            );
            sendMessage(tcpSocket, message);
          } catch (e) {
            console.error(
              `[${connectionId}] Failed to process client message, disconnecting. ${e}`
            );
            disconnect();
          }
        });
        wsClient.resume();

        while (tcpSocket.readable) {
          try {
            const message = await receiveMessage(tcpSocket);
            console.log(
              `[${connectionId}] Message from server`,
              trimMessage(message)
            );
            wsClient.send(message);
          } catch (e) {
            console.error(
              `[${connectionId}] Failed to process server message, disconnecting. ${e}`
            );
            disconnect();
          }
        }
      }
    );

    wsClient.on("close", () => {
      console.log(`[${connectionId}] Client disconnected`);
      disconnect();
    });

    tcpSocket.on("close", () => {
      console.log(`[${connectionId}] Server disconnected`);
      disconnect();
    });

    tcpSocket.on("error", (e) => {
      console.error(`[${connectionId}] Server error, disconnecting. ${e}`);
      disconnect();
    });
  });
}

function trimMessage(message, maxLength = 400) {
  return message.length > maxLength
    ? message.substring(0, maxLength - 3) + "..."
    : message;
}

async function validateSignature(connectionId, tcpSocket) {
  tcpSocket.write(new Uint8Array(PROTOCOL_SIGNATURE));
  const signature = await readSocketAsync(tcpSocket, 16);
  const isSignatureMatch = isArrayEquals(signature, PROTOCOL_SIGNATURE);

  if (!isSignatureMatch) {
    throw new Error("Signature mismatch");
  }

  console.log(`[${connectionId}] Signature is valid`);
}

async function checkCompatibility(connectionId, tcpSocket) {
  const request = {
    id: uuid(),
    command: "173cbbeb-1d81-4e01-bf3c-5d06f9c878c3",
    flags: 2164291649,
    content: null,
    tags: null,
  };
  let response;

  try {
    await sendMessage(tcpSocket, JSON.stringify(request));
    response = JSON.parse(await receiveMessage(tcpSocket)) || {};
  } catch (e) {
    throw new Error(`Failed to check compatibility. ${e}`);
  }

  if (response.command !== request.command) {
    throw new Error(
      `Protocol is not compatible, server response: ${response.command}`
    );
  }

  console.log(`[${connectionId}] Protocol is compatible`);
}

async function receiveMessage(socket) {
  const sizeBuffer = await readSocketAsync(socket, 4);
  const size = sizeBuffer.readUInt32BE(0);

  const messageBuffer = await readSocketAsync(socket, size);
  return decoder.decode(messageBuffer);
}

async function sendMessage(socket, message) {
  const bytes = encoder.encode(message);
  const buffer = Buffer.alloc(4 + bytes.length);
  buffer.writeUInt32BE(bytes.length, 0);
  buffer.set(bytes, 4);
  await writeSocketAsync(socket, buffer);
}

function writeSocketAsync(socket, data) {
  return new Promise((resolve) => {
    socket.write(data, resolve);
  });
}

function readSocketAsync(socket, size) {
  return new Promise((resolve) => {
    readSocket(socket, size, resolve);
  });
}

function readSocket(socket, size, cb) {
  const result = socket.read(size);

  if (result === null) {
    socket.once("readable", () => {
      readSocket(socket, size, cb);
    });
  } else {
    cb(result);
  }
}

function isArrayEquals(a, b) {
  if (a.length !== b.length) return false;

  for (let i = 0; i < a.length; ++i) {
    if (a[i] !== b[i]) return false;
  }

  return true;
}

function randomId() {
  return Math.round(Math.random() * 1000000000).toString(36);
}

main();
