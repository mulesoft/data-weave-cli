# Node.js API for DataWeave Native Library — Implementation Plan

## Overview

Add a Node.js package (`@dataweave/native`) that mirrors the existing Python API, exposing the DataWeave native shared library (`dwlib`) via a N-API native addon. The package will be built as a platform-specific tarball (`.tgz`) and uploaded to GitHub Releases alongside the Python wheel.

## Architecture Decisions

### FFI Binding: N-API Native Addon (C)

**Critical finding**: `koffi` (pure JS FFI) does not work with GraalVM Native Image in Node.js due to a fundamental signal handler conflict between V8 and GraalVM. Both engines install SIGSEGV handlers, causing crashes during isolate initialization regardless of which thread the call originates from.

The solution is a N-API native addon written in C that runs ALL GraalVM calls on dedicated pthreads with sufficient stack size, completely isolated from V8's signal handling.

| Option | Status | Notes |
|--------|--------|-------|
| `koffi` | ❌ REJECTED | V8/GraalVM signal handler conflict causes StackOverflowError during `graal_create_isolate` |
| N-API addon (C) | ✅ CHOSEN | Full control over thread creation, stack sizes, and signal isolation |
| `node-ffi-napi` | ❌ | Same V8 signal handler conflict as koffi |

Key technical constraints:
- `graal_create_isolate` must run on a dedicated thread with 16MB stack (not a V8/libuv worker thread)
- All subsequent GraalVM calls must use `graal_attach_thread`/`graal_detach_thread` on dedicated threads
- Threading uses libuv (`uv_thread_create_ex`, `uv_mutex`, `uv_cond`, `uv_dlopen`) for cross-platform Windows/Linux/macOS support — no POSIX-only APIs
- Streaming uses `napi_threadsafe_function` to bridge native thread callbacks back to the JS event loop
- Sentinel values are sent through the same tsfn queue as data chunks to guarantee delivery ordering (avoids race between `napi_async_work` completion and pending tsfn dispatches)
- Reference counting on `initialize`/`cleanup` allows multiple `DataWeave` instances to share a single GraalVM isolate
- The addon uses the raw C `<node_api.h>` header directly (not the `node-addon-api` C++ wrapper) to avoid MSVC `/std:c++17` conflicts on Windows

### Package Layout

```
native-lib/
└── node/
    ├── package.json
    ├── tsconfig.json
    ├── binding.gyp          # node-gyp build config for native addon
    ├── src/
    │   ├── addon.c          # N-API native addon (libuv threads + GraalVM FFI)
    │   ├── index.ts         # Public API (module-level + class)
    │   ├── ffi.ts           # TypeScript wrapper loading .node addon
    │   ├── types.ts         # TypeScript interfaces & types
    │   └── utils.ts         # Input normalization, library path resolution
    ├── tests/
    │   ├── dataweave.test.ts
    │   └── fixtures/
    │       └── person.xml
    ├── native/              # Staged dwlib.* (gitignored, populated by Gradle)
    │   └── .gitkeep
    └── dist/                # Compiled JS output (gitignored)
```

### API Design (mirrors Python)

```typescript
// Module-level convenience (lazy-initializes a global instance)
import { run, runStreaming, runTransform, cleanup } from '@dataweave/native';

const result = run('2 + 2');
console.log(result.getString()); // "4"

// Streaming output (AsyncGenerator)
for await (const chunk of runStreaming('output json --- (1 to 10000) map {id: $}')) {
  process.stdout.write(chunk);
}

// Bidirectional streaming
import { createReadStream } from 'fs';
const output = runTransform(
  'output csv --- payload',
  createReadStream('large.json'),
  { mimeType: 'application/json' }
);
for await (const chunk of output) {
  process.stdout.write(chunk);
}

// Explicit lifecycle
import { DataWeave } from '@dataweave/native';
const dw = new DataWeave();
dw.initialize();
const result = dw.run('2 + 2');
dw.cleanup();
```

## Implementation Status

### ✅ Phase 1: Core Package Structure
- `package.json` with node-gyp, vitest, typescript (no runtime dependencies)
- `tsconfig.json` (CommonJS, ES2022, strict)
- `binding.gyp` (C11 on macOS/Linux, CompileAs=C on Windows, NAPI_VERSION=8)

### ✅ Phase 2: Native Addon (`src/addon.c`)
- All GraalVM calls on dedicated threads via libuv (`uv_thread_create_ex`)
- `initialize`: thread with 16MB stack → `graal_create_isolate`
- `runScript`: thread with 2MB stack → `attach_thread` + `run_script` + `detach_thread`
- `runScriptStreaming`: thread + `napi_threadsafe_function` + sentinel for ordered delivery
- `runScriptTransform`: bidirectional with read tsfn (blocking `uv_cond`) + write tsfn + sentinel
- `cleanup`: thread → `graal_tear_down_isolate`, ref-counted
- Library loading via `uv_dlopen`/`uv_dlsym` (cross-platform)

### ✅ Phase 3: TypeScript FFI wrapper (`src/ffi.ts`)
- Loads `.node` addon via `require()`
- Thin typed wrapper

### ✅ Phase 4: Library resolution (`src/utils.ts`)
- Same search order as Python: env var → `native/` → build dir → CWD

### ✅ Phase 5: Public API (`src/index.ts`)
- `DataWeave` class: `initialize()`, `cleanup()`, `run()`, `runStreaming()`, `runTransform()`
- Module-level convenience functions with lazy singleton
- `runStreaming` → `AsyncGenerator<Buffer, StreamingResult>`
- `runTransform` → accepts `AsyncIterable<Buffer>`, returns `AsyncGenerator<Buffer, StreamingResult>`

### ✅ Phase 6: Tests (14/14 passing)
- Basic arithmetic, inputs, explicit instance lifecycle
- UTF-16 XML encoding, auto-conversion, error handling
- Streaming: basic, large output, error propagation, with inputs
- Transform: basic bidirectional, large chunked input, file-based

### ✅ Phase 7: Gradle Integration
- `stageNodeNativeLib` — copies dwlib.* to node/native/
- `buildNodePackage` — npm install + node-gyp rebuild + tsc + npm pack
- `nodeTest` — full test run (skippable with `-PskipNodeTests=true`)
- `clean` updated to remove node artifacts

### ✅ Phase 8: GitHub Actions CI
- Initial `./gradlew build` runs with `-PskipNodeTests=true` (Node.js not yet available)
- Setup Node.js 18 via `actions/setup-node@v4`
- `./gradlew native-lib:buildNodePackage` (stage + compile addon + tsc + npm pack)
- `./gradlew native-lib:nodeTest` (explicit test run after Node.js setup)
- Upload `dataweave-native-0.0.1.tgz` per OS

## Technical Details

### Threading Model

```
JS Main Thread              Dedicated threads (via libuv)
─────────────────          ──────────────────────────────
initialize() ──────────►   uv_thread(16MB stack)
                               uv_dlopen() + graal_create_isolate()
                           ◄── return

runScript() ───────────►   uv_thread(2MB stack)
                               graal_attach_thread()
                               run_script()
                               graal_detach_thread()
                           ◄── return result

runScriptStreaming() ──►   uv_thread(2MB stack)
                               graal_attach_thread()
                               run_script_callback(write_cb)
                                 │
                                 ├── write_cb: chunk → tsfn queue
                                 ├── write_cb: chunk → tsfn queue
                                 └── sentinel(-1) → tsfn queue
                           
                           tsfn dispatches on JS thread:
                             chunk → JS callback
                             chunk → JS callback
                             sentinel → resolve promise

cleanup() ─────────────►   uv_thread(2MB stack)
                               graal_tear_down_isolate()
                           ◄── return
```

### Sentinel-based Ordering

The async streaming resolution uses a sentinel (chunk with `len == -1`) sent through the same `napi_threadsafe_function` queue as data chunks. This guarantees the promise resolves only after ALL data chunks have been delivered to JS, avoiding the race condition that occurs when using `napi_async_work` completion callbacks (which can fire before pending tsfn items are dispatched).

### Reference Counting

Multiple `DataWeave` instances share a single GraalVM isolate. Each `initialize()` increments a ref count; `cleanup()` decrements it. The isolate is only torn down when the last reference is released.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| V8/GraalVM signal handler conflict | All GraalVM calls on dedicated libuv threads (verified working) |
| Thread stack overflow | 16MB for init, 2MB for calls (matches GraalVM requirements) |
| Streaming ordering race | Sentinel through same tsfn queue (verified working) |
| Windows MSVC `/std:c++17` conflict | Removed `node-addon-api` C++ dep; use raw `node_api.h` C header + `CompileAs=1` (`/TC`) |
| Cross-platform threading | libuv primitives (`uv_thread`, `uv_mutex`, `uv_cond`, `uv_dlopen`) — no POSIX deps |
| node-gyp required at install | Package includes pre-built .node + source for rebuild |
