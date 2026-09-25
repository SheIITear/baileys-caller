import { test } from "node:test";
import assert from "node:assert/strict";
import { ActiveCall } from "../dist/index.mjs";

const engine = { endCall() {}, setMute() {} };
const ended = (call) =>
  new Promise((resolve) => call.once("ended", resolve));

test("end() emits ended when the stack confirms the hang-up", async () => {
  const call = new ActiveCall("id", engine, 0);
  const seen = ended(call);
  call.end();
  call._updateState(0); // CallState.Idle, as the stack reports it
  assert.equal(await seen, "ended");
  assert.equal(await call.waitForEnd(), "ended");
});

test("end() still emits ended when the stack never confirms", async () => {
  const call = new ActiveCall("id", engine, 0);
  const seen = ended(call);
  call.end();
  assert.equal(await seen, "hangup"); // would hang forever before the fix
});

test("a remote hang-up after a local end() is not swallowed", async () => {
  const call = new ActiveCall("id", engine, 0);
  const seen = ended(call);
  call.end();
  call._forceEnd("remote_end");
  assert.equal(await seen, "remote_end");
});

test("the duration cap ends the call", async () => {
  const call = new ActiveCall("id", engine, 50);
  assert.equal(await call.waitForEnd(), "hangup");
});

test("ended fires once", async () => {
  const call = new ActiveCall("id", engine, 0);
  let count = 0;
  call.on("ended", () => (count += 1));
  call.end();
  call._forceEnd("remote_end");
  call._updateState(0);
  await new Promise((r) => setTimeout(r, 3000));
  assert.equal(count, 1);
});
