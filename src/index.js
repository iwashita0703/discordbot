require("dotenv").config();

const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream");
const { spawn } = require("child_process");
const {
  Client,
  GatewayIntentBits,
  Partials
} = require("discord.js");
const {
  EndBehaviorType,
  VoiceConnectionStatus,
  entersState,
  joinVoiceChannel
} = require("@discordjs/voice");
const prism = require("prism-media");

const token = process.env.DISCORD_TOKEN;
if (!token) {
  throw new Error("DISCORD_TOKEN is required.");
}

const recordingsDir = process.env.RECORDINGS_DIR || path.join(process.cwd(), "recordings");
const sessions = new Map();
const PCM_SAMPLE_RATE = 48000;
const PCM_CHANNELS = 2;
const PCM_BYTES_PER_SAMPLE = 2;
const PCM_FRAME_BYTES = PCM_CHANNELS * PCM_BYTES_PER_SAMPLE;
const PCM_BYTES_PER_SECOND = PCM_SAMPLE_RATE * PCM_FRAME_BYTES;
const MIX_CHUNK_BYTES = PCM_BYTES_PER_SECOND;

fs.mkdirSync(recordingsDir, { recursive: true });

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
  partials: [Partials.Channel]
});

class RecordingSession {
  constructor({ interaction, connection, voiceChannel }) {
    this.guild = interaction.guild;
    this.guildId = interaction.guildId;
    this.textChannel = interaction.channel;
    this.connection = connection;
    this.receiver = connection.receiver;
    this.voiceChannel = voiceChannel;
    this.startedBy = interaction.user.id;
    this.startedAt = Date.now();
    this.stopped = false;
    this.activePipelines = new Set();
    this.activeSpeakerStreams = new Set();
    this.segments = [];
    this.sessionId = new Date(this.startedAt)
      .toISOString()
      .replace(/[:.]/g, "-");
    this.sessionDir = path.join(recordingsDir, `${this.guildId}-${this.sessionId}`);
    this.segmentsDir = path.join(this.sessionDir, "segments");
    this.handleSpeakingStart = this.handleSpeakingStart.bind(this);

    fs.mkdirSync(this.segmentsDir, { recursive: true });
  }

  start() {
    this.receiver.speaking.on("start", this.handleSpeakingStart);
    console.log(`Recording started in guild ${this.guildId}, channel ${this.voiceChannel.id}.`);
  }

  handleSpeakingStart(userId) {
    if (this.stopped) return;
    if (this.activeSpeakerStreams.has(userId)) {
      return;
    }

    try {
      this.activeSpeakerStreams.add(userId);
      const startOffsetMs = Date.now() - this.startedAt;
      const segmentIndex = this.segments.length + 1;
      const fileName = `${String(segmentIndex).padStart(4, "0")}-${userId}-${startOffsetMs}.pcm`;
      const filePath = path.join(this.segmentsDir, fileName);
      const segment = {
        index: segmentIndex,
        userId,
        startOffsetMs,
        file: path.relative(this.sessionDir, filePath).replace(/\\/g, "/"),
        startedAt: new Date().toISOString()
      };

      this.segments.push(segment);
      console.log(`Recording segment ${segment.index} from user ${userId}.`);

      const opusStream = this.receiver.subscribe(userId, {
        end: {
          behavior: EndBehaviorType.AfterSilence,
          duration: 1000
        }
      });
      const decoder = new prism.opus.Decoder({
        frameSize: 960,
        channels: 2,
        rate: 48000
      });
      const output = fs.createWriteStream(filePath);

      const done = new Promise((resolve) => {
        pipeline(opusStream, decoder, output, (error) => {
          segment.endedAt = new Date().toISOString();
          if (fs.existsSync(filePath)) {
            segment.bytes = fs.statSync(filePath).size;
            segment.durationMs = Math.round((segment.bytes / 192000) * 1000);
          }
          if (error) {
            segment.error = error.message;
            console.error(`Segment ${segment.index} failed:`, error);
          }
          this.activeSpeakerStreams.delete(userId);
          resolve();
        });
      });

      this.activePipelines.add(done);
      done.finally(() => {
        this.activePipelines.delete(done);
        this.activeSpeakerStreams.delete(userId);
      });
    } catch (error) {
      this.activeSpeakerStreams.delete(userId);
      console.error("Failed to start recording segment:", error);
    }
  }

  async stop() {
    this.stopped = true;
    this.receiver.speaking.off("start", this.handleSpeakingStart);
    this.connection.destroy();

    await waitForPipelines(this.activePipelines, 5000);

    const manifestPath = path.join(this.sessionDir, "manifest.json");
    const mixedPath = path.join(this.sessionDir, "mixed.ogg");
    const manifest = {
      guildId: this.guildId,
      voiceChannelId: this.voiceChannel.id,
      voiceChannelName: this.voiceChannel.name,
      startedBy: this.startedBy,
      startedAt: new Date(this.startedAt).toISOString(),
      stoppedAt: new Date().toISOString(),
      segmentCount: this.segments.length,
      segments: this.segments
    };

    fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

    if (this.segments.length > 0) {
      await mixSegments(this.sessionDir, this.segments, mixedPath);
    }

    return {
      sessionDir: this.sessionDir,
      manifestPath,
      mixedPath: fs.existsSync(mixedPath) ? mixedPath : null,
      segmentCount: this.segments.length
    };
  }
}

client.once("ready", () => {
  console.log(`Logged in as ${client.user.tag}.`);
});

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand() || !interaction.guild) return;

  if (interaction.commandName === "record") {
    await handleRecord(interaction);
    return;
  }

  if (interaction.commandName === "stop") {
    await handleStop(interaction);
  }
});

async function handleRecord(interaction) {
  if (sessions.has(interaction.guildId)) {
    await interaction.reply({
      content: "This server is already recording. Use `/stop` first.",
      ephemeral: true
    });
    return;
  }

  const member = await interaction.guild.members.fetch(interaction.user.id);
  const voiceChannel = member.voice.channel;
  if (!voiceChannel) {
    await interaction.reply({
      content: "Join the voice channel you want to record first.",
      ephemeral: true
    });
    return;
  }

  await interaction.deferReply();

  const connection = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId: interaction.guildId,
    adapterCreator: interaction.guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: true
  });

  try {
    await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
  } catch (error) {
    connection.destroy();
    await interaction.editReply(`Failed to join the voice channel: ${error.message}`);
    return;
  }

  const session = new RecordingSession({ interaction, connection, voiceChannel });
  sessions.set(interaction.guildId, session);
  session.start();

  await interaction.editReply(
    `Recording started in ${voiceChannel.name}.\nMake sure everyone in the channel has consented. Use \`/stop\` to finish.`
  );
}

async function handleStop(interaction) {
  const session = sessions.get(interaction.guildId);
  if (!session) {
    await interaction.reply({
      content: "This server is not currently recording.",
      ephemeral: true
    });
    return;
  }

  await interaction.deferReply();
  sessions.delete(interaction.guildId);

  try {
    const result = await session.stop();
    const lines = [
      "Recording stopped.",
      `Voice segments: ${result.segmentCount}`,
      `Saved to: ${result.sessionDir}`
    ];

    if (!result.mixedPath) {
      await interaction.editReply(`${lines.join("\n")}\nNo audio was recorded.`);
      return;
    }

    const mixedStats = fs.statSync(result.mixedPath);
    lines.push(`File: ${result.mixedPath}`);
    lines.push(`Size: ${formatBytes(mixedStats.size)}`);
    lines.push("Saved locally only. No file was attached to Discord.");

    await interaction.editReply(lines.join("\n"));
  } catch (error) {
    await interaction.editReply(`Failed while stopping the recording: ${error.message}`);
  }
}

async function waitForPipelines(activePipelines, timeoutMs) {
  if (activePipelines.size === 0) return;

  await Promise.race([
    Promise.allSettled([...activePipelines]),
    new Promise((resolve) => setTimeout(resolve, timeoutMs))
  ]);
}

async function mixSegments(sessionDir, segments, outputPath) {
  const preparedSegments = segments
    .map((segment) => {
      const segmentPath = path.join(sessionDir, segment.file);
      if (!isUsableAudioSegment(segmentPath)) {
        return null;
      }

      const segmentSize = alignToFrame(fs.statSync(segmentPath).size);
      const offsetBytes = startOffsetToBytes(segment.startOffsetMs);
      return {
        path: segmentPath,
        offsetBytes,
        size: segmentSize,
        endBytes: offsetBytes + segmentSize,
        fd: null
      };
    })
    .filter(Boolean)
    .sort((a, b) => a.offsetBytes - b.offsetBytes);

  if (preparedSegments.length === 0) return;

  const totalBytes = Math.max(...preparedSegments.map((segment) => segment.endBytes));
  const mixedPcmPath = path.join(sessionDir, "mixed.pcm");

  try {
    await writeMixedPcm(preparedSegments, totalBytes, mixedPcmPath);

    await runCommand("ffmpeg", [
      "-hide_banner",
      "-loglevel",
      "error",
      "-y",
      "-f",
      "s16le",
      "-ar",
      String(PCM_SAMPLE_RATE),
      "-ac",
      String(PCM_CHANNELS),
      "-i",
      mixedPcmPath,
      "-c:a",
      "libopus",
      "-b:a",
      "96k",
      outputPath
    ]);
  } finally {
    fs.rmSync(mixedPcmPath, { force: true });
  }
}

function isUsableAudioSegment(segmentPath) {
  return fs.existsSync(segmentPath) && fs.statSync(segmentPath).size > 3840;
}

async function writeMixedPcm(segments, totalBytes, mixedPcmPath) {
  let outputFd = null;
  let activeSegments = [];
  let nextSegmentIndex = 0;

  try {
    outputFd = fs.openSync(mixedPcmPath, "w");

    for (let chunkStart = 0; chunkStart < totalBytes; chunkStart += MIX_CHUNK_BYTES) {
      const chunkBytes = alignToFrame(Math.min(MIX_CHUNK_BYTES, totalBytes - chunkStart));
      const chunkEnd = chunkStart + chunkBytes;

      while (
        nextSegmentIndex < segments.length &&
        segments[nextSegmentIndex].offsetBytes < chunkEnd
      ) {
        const segment = segments[nextSegmentIndex];
        segment.fd = fs.openSync(segment.path, "r");
        activeSegments.push(segment);
        nextSegmentIndex += 1;
      }

      activeSegments = activeSegments.filter((segment) => {
        if (segment.endBytes <= chunkStart) {
          closeSegmentFd(segment);
          return false;
        }
        return true;
      });

      const mixedSamples = new Int32Array(chunkBytes / PCM_BYTES_PER_SAMPLE);

      for (const segment of activeSegments) {
        const overlapStart = Math.max(chunkStart, segment.offsetBytes);
        const overlapEnd = Math.min(chunkEnd, segment.endBytes);
        const readBytes = alignToFrame(overlapEnd - overlapStart);
        if (readBytes <= 0) continue;

        const readBuffer = Buffer.allocUnsafe(readBytes);
        const bytesRead = fs.readSync(
          segment.fd,
          readBuffer,
          0,
          readBytes,
          overlapStart - segment.offsetBytes
        );
        const targetSampleStart = (overlapStart - chunkStart) / PCM_BYTES_PER_SAMPLE;

        for (let byteIndex = 0; byteIndex + 1 < bytesRead; byteIndex += PCM_BYTES_PER_SAMPLE) {
          mixedSamples[targetSampleStart + byteIndex / PCM_BYTES_PER_SAMPLE] +=
            readBuffer.readInt16LE(byteIndex);
        }
      }

      const outputBuffer = Buffer.allocUnsafe(chunkBytes);
      let peak = 0;
      for (const sample of mixedSamples) {
        const absoluteSample = Math.abs(sample);
        if (absoluteSample > peak) peak = absoluteSample;
      }

      const scale = peak > 32767 ? 32767 / peak : 1;
      for (let sampleIndex = 0; sampleIndex < mixedSamples.length; sampleIndex += 1) {
        outputBuffer.writeInt16LE(
          clipInt16(Math.round(mixedSamples[sampleIndex] * scale)),
          sampleIndex * PCM_BYTES_PER_SAMPLE
        );
      }

      fs.writeSync(outputFd, outputBuffer, 0, outputBuffer.length);
    }
  } finally {
    if (outputFd !== null) fs.closeSync(outputFd);
    activeSegments.forEach(closeSegmentFd);
  }
}

function startOffsetToBytes(startOffsetMs) {
  const offsetMs = Number.isFinite(startOffsetMs) ? startOffsetMs : 0;
  const offsetBytes = Math.floor((Math.max(0, offsetMs) / 1000) * PCM_BYTES_PER_SECOND);
  return alignToFrame(offsetBytes);
}

function alignToFrame(bytes) {
  return Math.max(0, bytes - (bytes % PCM_FRAME_BYTES));
}

function closeSegmentFd(segment) {
  if (segment.fd === null) return;
  fs.closeSync(segment.fd);
  segment.fd = null;
}

function clipInt16(value) {
  return Math.max(-32768, Math.min(32767, value));
}

function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

function runCommand(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { windowsHide: true });
    let stderr = "";

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve();
        return;
      }

      reject(new Error(`${command} exited with ${code}: ${stderr.trim()}`));
    });
  });
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

async function shutdown() {
  for (const session of sessions.values()) {
    await session.stop().catch((error) => console.error(error));
  }
  process.exit(0);
}

client.login(token);
