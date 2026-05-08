#!/usr/bin/env node

import { runTransform, cleanup } from "./node/dist/index.js";

function formatBytes(bytes) {
  return (bytes / 1048576).toFixed(1);
}

function getRSS() {
  return process.memoryUsage().rss;
}

function* inputChunks(numElements) {
  const bufSize = 8192;
  let i = 0;
  let started = false;
  let pendingToken = null;

  while (i < numElements || pendingToken !== null) {
    const parts = [];
    if (!started) {
      parts.push(Buffer.from("["));
      started = true;
    }
    let remaining = bufSize - parts.reduce((sum, p) => sum + p.length, 0);

    if (pendingToken !== null) {
      if (pendingToken.length <= remaining) {
        parts.push(pendingToken);
        remaining -= pendingToken.length;
        pendingToken = null;
      } else {
        yield Buffer.concat(parts);
        continue;
      }
    }

    while (remaining > 0 && i < numElements) {
      const token = Buffer.from((i > 0 ? "," : "") + String(i));
      if (token.length > remaining) {
        pendingToken = token;
        break;
      }
      parts.push(token);
      remaining -= token.length;
      i++;
    }

    if (i >= numElements && pendingToken === null) {
      parts.push(Buffer.from("]"));
    }

    if (parts.length > 0) {
      yield Buffer.concat(parts);
    }
  }

  // If we exited without closing bracket (pending was last)
  // This shouldn't happen but just in case
}

async function exampleRunTransform() {
  console.log("\nTesting streaming input and output using runTransform (square numbers)...");

  const startTime = process.hrtime.bigint();
  const numElements = 1_000_000 * 50;

  const script = `output application/json deferred=true
---
payload map ($ * $)`;

  const startRSS = getRSS();
  console.log(`>>> Before runTransform, RSS: ${formatBytes(startRSS)} MB`);

  const gen = runTransform(script, inputChunks(numElements), {
    mimeType: "application/json",
    charset: "utf-8",
  });

  let chunkCount = 0;
  let totalBytes = 0;
  let result = await gen.next();

  while (!result.done) {
    chunkCount++;
    totalBytes += result.value.length;
    if (chunkCount % 5000 === 0) {
      const rss = getRSS();
      console.log(
        `--- chunk ${chunkCount}: ${result.value.length} bytes, total: ${formatBytes(totalBytes)} MB, RSS: ${formatBytes(rss)} MB ---`
      );
    }
    result = await gen.next();
  }

  const metadata = result.value;
  if (!metadata.success) {
    throw new Error(metadata.error || "Unknown error");
  }

  const elapsed = Number(process.hrtime.bigint() - startTime) / 1e9;
  const mins = Math.floor(elapsed / 60);
  const secs = (elapsed % 60).toFixed(3).padStart(6, "0");
  const peakRSS = getRSS();

  console.log(
    `\n[OK] runTransform done (${chunkCount} chunks, ${formatBytes(totalBytes)} MB, ${numElements.toLocaleString()} elements) - Time: ${mins}:${secs}`
  );
  console.log(`RSS at end: ${formatBytes(peakRSS)} MB`);
}

async function main() {
  console.log("=".repeat(70));
  console.log("Node.js runTransform (AsyncGenerator API)");
  console.log("=".repeat(70));

  try {
    await exampleRunTransform();
  } catch (e) {
    console.error(`[FAIL] runTransform failed: ${e.message}`);
    console.error(e.stack);
  } finally {
    cleanup();
  }
}

main();
