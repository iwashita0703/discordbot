FROM node:22-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates ffmpeg \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY src ./src

ENV NODE_ENV=production
ENV RECORDINGS_DIR=/recordings

RUN mkdir -p /recordings
VOLUME ["/recordings"]

CMD ["npm", "start"]
