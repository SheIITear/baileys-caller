import { test } from "node:test";
import assert from "node:assert/strict";
import v8 from "node:v8";
import vm from "node:vm";
import { WasmEngine } from "../dist/wasm-engine.mjs";

v8.setFlagsFromString("--expose-gc");
const gc = vm.runInNewContext("gc");

const settle = async () => {
  for (let i = 0; i < 6; i += 1) {
    gc();
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
};

const smallEngine = () => new WasmEngine({ pthreadPoolSize: 1, runtimePthreadPoolSize: 2, enableLogs: false, callbacks: {} });

test("pool sizes are configurable", async () => {
  const engine = smallEngine();
  await engine.initialize();
  try {
    assert.deepEqual(engine.workerStats(), { pool: 1, running: 0, unused: 1, dedicated: 2 });
  } finally {
    engine.destroy();
  }
});

test("destroy() terminates every worker and clears the process-wide callbacks", async () => {
  const engine = smallEngine();
  await engine.initialize();
  engine.destroy();
  assert.deepEqual(engine.workerStats(), { pool: 1, running: 0, unused: 0, dedicated: 0 });
  assert.equal(globalThis.WhatsAppVoipWasmCallbacks, undefined);
  assert.equal(globalThis.WhatsAppVoipWasmWorkerCompatibleCallbacks, undefined);
});

test("a destroyed engine can be garbage collected", async () => {
  // A detach/attach cycle used to leave the old engine (and its WASM module
  // and shared memory) reachable forever: through static callback listeners
  // and through waitAsync promises that nothing would ever wake.
  let ref;
  await (async () => {
    const engine = smallEngine();
    await engine.initialize();
    engine.destroy();
    ref = new WeakRef(engine);
  })();
  // A second engine must not keep the first one alive either.
  const next = smallEngine();
  await next.initialize();
  try {
    await settle();
    assert.equal(ref.deref(), undefined);
  } finally {
    next.destroy();
  }
});

test("the VoIP stack reports ready without waiting out the timeout", async () => {
  // The runtime never calls onVoipReady; readiness used to take the full 15 s.
  const engine = smallEngine();
  await engine.initialize();
  try {
    const started = Date.now();
    engine.initVoipStack("15550001111@s.whatsapp.net", "15550001111@s.whatsapp.net", "123456789012345@lid");
    await engine.waitForVoipStackReady();
    assert.ok(Date.now() - started < 5000, `took ${Date.now() - started} ms`);
  } finally {
    engine.destroy();
  }
});
