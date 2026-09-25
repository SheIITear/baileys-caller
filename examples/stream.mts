/**
 * Example: attach to a socket you own and stream live audio into a call.
 *
 *   npx tsx examples/stream.mts <authDir> <phoneNumber>
 *
 * Plays a 440 Hz tone pushed through `call.pushAudio()` and logs how much
 * audio arrives from the remote side. This is the shape a realtime voice
 * bridge uses: your app keeps the Baileys socket (auth, reconnects), and
 * baileys-caller only adds the VoIP stack on top of it.
 */
import makeWASocket, { useMultiFileAuthState } from "@whiskeysockets/baileys";
import { VoipClient } from "../src/index.mjs";

const [, , authDir = "./auth", phoneNumber] = process.argv;
if (!phoneNumber) {
  console.error("Usage: npx tsx examples/stream.mts <authDir> <phoneNumber>");
  process.exit(1);
}

const { state, saveCreds } = await useMultiFileAuthState(authDir);
const sock = makeWASocket({ auth: state });
sock.ev.on("creds.update", saveCreds);
await new Promise<void>((resolve, reject) => {
  sock.ev.on("connection.update", (u) => {
    if (u.connection === "open") resolve();
    if (u.connection === "close") reject(u.lastDisconnect?.error ?? new Error("closed"));
  });
});

const client = new VoipClient();
await client.attach(sock);

const call = await client.call(phoneNumber, { audioSource: "stream", durationMs: 20_000 });

const SAMPLE_RATE = 16_000;
let phase = 0;
const tone = setInterval(() => {
  // Keep ~200 ms queued: push 100 ms whenever the buffer runs low.
  if (call.queuedAudioChunks > 10) return;
  const pcm = new Float32Array(SAMPLE_RATE / 10);
  for (let i = 0; i < pcm.length; i++) {
    pcm[i] = 0.2 * Math.sin((2 * Math.PI * 440 * phase++) / SAMPLE_RATE);
  }
  call.pushAudio(pcm);
}, 20);

let received = 0;
call.on("audio", (pcm) => { received += pcm.length; });
call.on("connected", () => console.log("Connected, streaming tone"));
call.on("ended", (reason) => console.log(`Ended: ${reason}, received ${(received / SAMPLE_RATE).toFixed(1)}s of audio`));

await call.waitForEnd();
clearInterval(tone);
client.detach(); // VoIP stack gone, socket still yours
sock.end(undefined);
