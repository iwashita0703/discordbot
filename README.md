# Discord Voice Recorder Bot

Docker で動く Discord ボイスチャンネル録音 bot です。`/record` で bot が通話に入り、`/stop` で録音を停止して `recordings/` に `mixed.ogg` を保存します。

録音は必ず参加者全員の同意を取ってから使ってください。サーバールールや地域の法律によっては禁止・制限される場合があります。

## Discord 側の準備

1. Discord Developer Portal で Application を作成します。
2. Bot を追加し、Token を発行します。
3. OAuth2 URL Generator で `bot` と `applications.commands` を選びます。
4. Bot Permissions で `Connect`, `View Channels`, `Send Messages`, `Use Slash Commands`, `Attach Files` を付けてサーバーに招待します。
5. Application ID、Bot Token、開発用サーバー ID を控えます。

## セットアップ

`.env.example` を `.env` にコピーして値を入れます。

```env
DISCORD_TOKEN=your_bot_token
DISCORD_CLIENT_ID=your_application_id
DISCORD_GUILD_ID=your_server_id

RECORDINGS_DIR=/recordings
MAX_UPLOAD_MB=8
```

スラッシュコマンドを登録します。

```powershell
docker compose run --rm discord-recorder npm run register
```

bot を起動します。

```powershell
docker compose up -d --build
```

ログを見る場合:

```powershell
docker compose logs -f
```

停止する場合:

```powershell
docker compose down
```

## 使い方

1. Discord のボイスチャンネルに入ります。
2. テキストチャンネルで `/record` を実行します。
3. 録音を止めたいときに `/stop` を実行します。
4. 録音結果は `recordings/<guild>-<timestamp>/mixed.ogg` に保存されます。

短い録音は Discord にも添付されます。大きい録音は `recordings/` にだけ保存されます。
