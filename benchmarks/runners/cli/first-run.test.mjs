import { test } from "node:test";
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { EventEmitter } from "node:events";
import { join } from "node:path";
import { runFirstRun, sampleOnce } from "./first-run.mjs";

const corpusDir = "/fake/corpus";
const script = join(corpusDir, "scripts/object-transform.dwl");
const inputPath = join(corpusDir, "inputs/person-record.json");
const manifest = {
  corpusDir,
  cases: [{
    id: "object-transform",
    script: "scripts/object-transform.dwl",
    inputs: {
      payload: { file: "inputs/person-record.json", mimeType: "application/json" },
    },
    metrics: ["first-run"],
  }],
};

function childProcess() {
  const child = new EventEmitter();
  child.stderr = new PassThrough();
  return child;
}

test("sampleOnce resolves elapsed time after a successful child close", async () => {
  const child = childProcess();
  const elapsed = await sampleOnce("/fake/dw", ["run"], manifest.cases[0], {
    spawnFn: () => {
      queueMicrotask(() => child.emit("close", 0));
      return child;
    },
  });

  assert.equal(typeof elapsed, "number");
  assert.ok(elapsed >= 0);
});

test("sampleOnce rejects a nonzero close with captured stderr", async () => {
  const child = childProcess();
  await assert.rejects(
    sampleOnce("/fake/dw", ["run"], manifest.cases[0], {
      spawnFn: () => {
        queueMicrotask(() => {
          child.stderr.write("invalid script");
          child.emit("close", 2);
        });
        return child;
      },
    }),
    /cli first-run failed for 'object-transform' \(exit 2\)\ninvalid script/,
  );
});

test("sampleOnce rejects a child spawn error", async () => {
  const child = childProcess();
  await assert.rejects(
    sampleOnce("/fake/dw", ["run"], manifest.cases[0], {
      spawnFn: () => {
        queueMicrotask(() => child.emit("error", new Error("not executable")));
        return child;
      },
    }),
    /cli first-run failed for 'object-transform'/,
  );
});

test("runFirstRun samples ordinary dw run arguments and aggregates samples", async () => {
  const rows = await runFirstRun(manifest, {
    sample: async (bin, args) => {
      assert.equal(bin, "/fake/dw");
      assert.deepEqual(args, ["run", "-i", `payload=${inputPath}`, "--file", script]);
      return 12.5;
    },
    binary: "/fake/dw",
    samplesOverride: 2,
  });

  assert.deepEqual(rows, [{
    id: "object-transform",
    metric: "first-run",
    unit: "ms",
    stats: { min: 12.5, median: 12.5, p90: 12.5, p99: 12.5, mean: 12.5 },
    iterations: 2,
  }]);
});

test("runFirstRun rejects instead of returning rows after a sample failure", async () => {
  await assert.rejects(
    runFirstRun(manifest, {
      sample: async () => {
        throw new Error("cli first-run failed for 'object-transform'");
      },
      binary: "/fake/dw",
      samplesOverride: 2,
    }),
    /cli first-run failed for 'object-transform'/,
  );
});
