FROM node:18-alpine

WORKDIR /app

ENV ORIGIN_PORT=28052
ENV ORIGIN_HOST=127.0.0.1
ENV PROXY_PORT=9000
ENV PROXY_HOST=0.0.0.0
ENV SOCKET_TIMEOUT=1000

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

COPY . .
EXPOSE 9000
CMD ["npm", "run", "start"]
