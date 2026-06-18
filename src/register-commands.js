require("dotenv").config();

const { REST, Routes, SlashCommandBuilder } = require("discord.js");

const token = process.env.DISCORD_TOKEN;
const clientId = process.env.DISCORD_CLIENT_ID;
const guildId = process.env.DISCORD_GUILD_ID;

if (!token || !clientId) {
  throw new Error("DISCORD_TOKEN and DISCORD_CLIENT_ID are required.");
}

const commands = [
  new SlashCommandBuilder()
    .setName("record")
    .setDescription("Join your current voice channel and start recording."),
  new SlashCommandBuilder()
    .setName("stop")
    .setDescription("Stop the current recording and save the mixed audio.")
].map((command) => command.toJSON());

const rest = new REST({ version: "10" }).setToken(token);

async function main() {
  if (guildId) {
    await rest.put(Routes.applicationGuildCommands(clientId, guildId), {
      body: commands
    });
    console.log(`Registered ${commands.length} guild commands for ${guildId}.`);
    return;
  }

  await rest.put(Routes.applicationCommands(clientId), { body: commands });
  console.log(`Registered ${commands.length} global commands.`);
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
