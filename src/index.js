require("dotenv").config();

const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream");
const { spawn } = require("child_process");
const {
  AttachmentBuilder,
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
const OpenAIImport = require("openai");

const OpenAI = OpenAIImport.OpenAI || OpenAIImport.default || OpenAIImport;

const token = process.env.DISCORD_TOKEN;
if (!token) {
  throw new Error("DISCORD_TOKEN is required.");
}

const recordingsDir = process.env.RECORDINGS_DIR || path.join(process.cwd(), "recordings");
const maxUploadBytes = Number(process.env.MAX_UPLOAD_MB || 24) * 1024 * 1024;
const transcriptionModel = process.env.TRANSCRIPTION_MODEL || "gpt-4o-mini-transcribe";
const transcriptionLanguage = process.env.TRANSCRIPTION_LANGUAGE || "ja";
const openai = process.env.OPENAI_API_KEY
  ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY })
  : null;
const sessions = new Map();

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

    let transcript = null;
    if (this.segments.length > 0) {
      transcript = await transcribeSegments({
        guild: this.guild,
        sessionDir: this.sessionDir,
        sessionInfo: manifest,
        segments: this.segments
      });
    }

    return {
      sessionDir: this.sessionDir,
      manifestPath,
      mixedPath: fs.existsSync(mixedPath) ? mixedPath : null,
      transcriptPath: transcript?.textPath || null,
      transcriptJsonPath: transcript?.jsonPath || null,
      transcriptionSkipped: transcript?.skippedReason || null,
      segmentCount: this.segments.length
    };
  }
}

client.once("ready", () => {
  console.log(`Logged in as ${client.user.tag}.`);
  if (!openai) {
    console.log("OPENAI_API_KEY is not set. Recording works, transcription will be skipped.");
  }
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

    if (result.transcriptPath) {
      lines.push(`Transcript: ${result.transcriptPath}`);
    } else if (result.transcriptionSkipped) {
      lines.push(`Transcript skipped: ${result.transcriptionSkipped}`);
    }

    if (!result.mixedPath) {
      await interaction.editReply(`${lines.join("\n")}\nNo audio was recorded.`);
      return;
    }

    const files = [];
    const mixedStats = fs.statSync(result.mixedPath);
    if (mixedStats.size <= maxUploadBytes) {
      files.push(new AttachmentBuilder(result.mixedPath, {
        name: path.basename(result.mixedPath)
      }));
    } else {
      lines.push("The mixed audio is too large to attach to Discord.");
    }

    if (result.transcriptPath && fs.statSync(result.transcriptPath).size <= maxUploadBytes) {
      files.push(new AttachmentBuilder(result.transcriptPath, {
        name: path.basename(result.transcriptPath)
      }));
    }

    await interaction.editReply({
      content: lines.join("\n"),
      files
    });
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
  const inputs = [];
  const filters = [];
  const labels = [];

  segments.forEach((segment) => {
    const segmentPath = path.join(sessionDir, segment.file);
    if (!isUsableAudioSegment(segmentPath)) {
      return;
    }

    inputs.push("-f", "s16le", "-ar", "48000", "-ac", "2", "-i", segmentPath);
    const inputIndex = labels.length;
    const label = `a${labels.length}`;
    const delay = Math.max(0, Math.floor(segment.startOffsetMs));
    filters.push(`[${inputIndex}:a]adelay=${delay}|${delay}[${label}]`);
    labels.push(`[${label}]`);
  });

  if (labels.length === 0) return;

  const filterComplex =
    labels.length === 1
      ? `${filters.join(";")};${labels[0]}alimiter=limit=0.95[out]`
      : `${filters.join(";")};${labels.join("")}amix=inputs=${labels.length}:duration=longest:normalize=0,alimiter=limit=0.95[out]`;

  await runCommand("ffmpeg", [
    "-hide_banner",
    "-loglevel",
    "error",
    "-y",
    ...inputs,
    "-filter_complex",
    filterComplex,
    "-map",
    "[out]",
    "-c:a",
    "libopus",
    "-b:a",
    "96k",
    outputPath
  ]);
}

async function transcribeSegments({ guild, sessionDir, sessionInfo, segments }) {
  const transcriptDir = path.join(sessionDir, "transcription");
  const textPath = path.join(sessionDir, "transcript.txt");
  const jsonPath = path.join(sessionDir, "transcript.json");

  if (!openai) {
    return { skippedReason: "OPENAI_API_KEY is not set" };
  }

  fs.mkdirSync(transcriptDir, { recursive: true });

  const speakerNames = await getSpeakerNames(guild, segments);
  const entries = [];
  let transcriptError = null;

  for (const segment of segments) {
    const segmentPath = path.join(sessionDir, segment.file);
    if (!isUsableAudioSegment(segmentPath)) {
      continue;
    }

    const audioPath = path.join(
      transcriptDir,
      `${String(segment.index).padStart(4, "0")}-${segment.userId}.ogg`
    );

    try {
      await convertPcmToOgg(segmentPath, audioPath);
      const text = await transcribeAudioFile(audioPath);
      const cleaned = text.trim();
      if (!cleaned) continue;

      entries.push({
        index: segment.index,
        startOffsetMs: segment.startOffsetMs,
        timestamp: formatTimestamp(segment.startOffsetMs),
        userId: segment.userId,
        speaker: speakerNames.get(segment.userId) || segment.userId,
        text: cleaned,
        audioFile: path.relative(sessionDir, audioPath).replace(/\\/g, "/")
      });
      console.log(`Transcribed segment ${segment.index}.`);
    } catch (error) {
      const normalizedError = normalizeTranscriptionError(error);
      entries.push({
        index: segment.index,
        startOffsetMs: segment.startOffsetMs,
        timestamp: formatTimestamp(segment.startOffsetMs),
        userId: segment.userId,
        speaker: speakerNames.get(segment.userId) || segment.userId,
        error: normalizedError
      });
      console.error(`Failed to transcribe segment ${segment.index}:`, error);

      if (isQuotaError(error)) {
        transcriptError = normalizedError;
        break;
      }
    }
  }

  const transcript = {
    model: transcriptionModel,
    language: transcriptionLanguage,
    speakerSeparation: "Discord voice receiver user IDs",
    session: sessionInfo,
    error: transcriptError,
    entries
  };

  fs.writeFileSync(jsonPath, JSON.stringify(transcript, null, 2));
  writeUtf8BomFile(textPath, renderTranscriptText(transcript));

  return { textPath, jsonPath, entries };
}

async function transcribeAudioFile(audioPath) {
  const request = {
    file: fs.createReadStream(audioPath),
    model: transcriptionModel
  };

  if (transcriptionLanguage) {
    request.language = transcriptionLanguage;
  }

  const result = await openai.audio.transcriptions.create(request);
  if (typeof result === "string") return result;
  return result.text || "";
}

async function convertPcmToOgg(inputPath, outputPath) {
  await runCommand("ffmpeg", [
    "-hide_banner",
    "-loglevel",
    "error",
    "-y",
    "-f",
    "s16le",
    "-ar",
    "48000",
    "-ac",
    "2",
    "-i",
    inputPath,
    "-c:a",
    "libopus",
    "-b:a",
    "48k",
    outputPath
  ]);
}

async function getSpeakerNames(guild, segments) {
  const names = new Map();
  const userIds = [...new Set(segments.map((segment) => segment.userId))];

  for (const userId of userIds) {
    try {
      const member = await guild.members.fetch(userId);
      names.set(userId, member.displayName || member.user.username || userId);
    } catch {
      names.set(userId, userId);
    }
  }

  return names;
}

function renderTranscriptText(transcript) {
  const lines = [
    "Discord Voice Recorder Transcript",
    `Started: ${transcript.session.startedAt}`,
    `Stopped: ${transcript.session.stoppedAt}`,
    `Channel: ${transcript.session.voiceChannelName} (${transcript.session.voiceChannelId})`,
    `Model: ${transcript.model}`,
    `Speaker separation: ${transcript.speakerSeparation}`,
    ""
  ];

  if (transcript.entries.length === 0) {
    lines.push("No speech was transcribed.");
  }

  if (transcript.error) {
    lines.push(`Transcription stopped: ${transcript.error}`);
    lines.push("");
  }

  for (const entry of transcript.entries) {
    if (entry.error) {
      lines.push(`[${entry.timestamp}] ${entry.speaker} (${entry.userId}): [transcription failed: ${entry.error}]`);
      continue;
    }

    lines.push(`[${entry.timestamp}] ${entry.speaker} (${entry.userId}): ${entry.text}`);
  }

  lines.push("");
  return lines.join("\n");
}

function normalizeTranscriptionError(error) {
  const message = error?.message || String(error);
  if (isQuotaError(error)) {
    return "OpenAI API quota exceeded. Check billing/credits for the API key.";
  }
  return message;
}

function isQuotaError(error) {
  return error?.status === 429 || /exceeded your current quota/i.test(error?.message || "");
}

function writeUtf8BomFile(filePath, text) {
  fs.writeFileSync(filePath, `\uFEFF${text}`, "utf8");
}

function isUsableAudioSegment(segmentPath) {
  return fs.existsSync(segmentPath) && fs.statSync(segmentPath).size > 3840;
}

function formatTimestamp(ms) {
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  return [hours, minutes, seconds]
    .map((value) => String(value).padStart(2, "0"))
    .join(":");
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
