FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache bash build-base ca-certificates curl docker-cli python3 py3-pip unzip util-linux

COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY . .

RUN /app/scripts/install-baidupcs-go.sh \
  && python3 -m pip install --break-system-packages --no-cache-dir bypy

CMD ["node", "/app/web/backend/server.mjs"]
