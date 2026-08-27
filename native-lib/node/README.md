# DataWeave Node.js Bindings

Node.js N-API bindings for the DataWeave native library. Execute DataWeave scripts directly from Node.js with full streaming and bidirectional I/O support.

## Prerequisites

1. **Node.js >= 18** (for N-API compatibility)
2. **Build the native library:**
   ```bash
   ./gradlew :native-lib:nativeCompile
   ```
3. The shared library will be at:
   - macOS: `native-lib/build/native/nativeCompile/dwlib.dylib`
   - Linux: `native-lib/build/native/nativeCompile/dwlib.so`
   - Windows: `native-lib/build/native/nativeCompile/dwlib.dll`

## Installation

### Option A: Install from package (recommended)

From npm:

```bash
npm install dataweave-native
```

Supported platform packages are `dataweave-native-linux-x64`,
`dataweave-native-win32-x64`, and `dataweave-native-darwin-arm64`.

After building locally, install the meta-package tarball and the matching platform tarball:

```bash
./gradlew :native-lib:buildNodePackage
npm install ./native-lib/node/dataweave-native-<ver>.tgz \
  ./native-lib/node/dataweave-native-<platform>-<ver>.tgz
```

### Option B: Install for development

```bash
./gradlew :native-lib:stageNodeNativeLib
cd native-lib/node
npm install
npm run build
```

### Option C: Use externally-built library via environment variable

```bash
export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib
cd native-lib/node
npm install
npm run build
```

## Quick Start

### Basic Script Execution

```javascript
import * as dataweave from 'dataweave-native';

const result = dataweave.run('2 + 2');
if (result.success) {
  console.log(result.getString());  // "4"
} else {
  console.error('Error:', result.error);
}
```

### Script with Inputs

Inputs can be plain JavaScript values (auto-encoded):

```javascript
const result = dataweave.run(
  'num1 + num2',
  { num1: 25, num2: 17 }
);
console.log(result.getString());  // "42"
```

### Error Handling

```javascript
const result = dataweave.run('invalid syntax', {}, { raiseOnError: true });
// Throws DataWeaveScriptError with result.error details
```

## API Reference

### Module-Level Functions

The module exports convenience functions that use a global singleton instance:

#### `run(script, inputs?, opts?): ExecutionResult`

Execute a DataWeave script and return the complete result.

```javascript
import { run } from 'dataweave-native';

const result = run(
  '%dw 2.0\noutput application/json\n---\npayload.items map $.price',
  { payload: { items: [{ price: 10 }, { price: 20 }] } }
);

if (result.success) {
  console.log(result.getString());  // "[10, 20]"
  console.log(result.mimeType);     // "application/json"
}
```

**Parameters:**
- `script` (string): DataWeave script source code
- `inputs` (object, optional): Input variables as key-value pairs
- `opts` (object, optional): Options
  - `raiseOnError` (boolean): Throw `DataWeaveScriptError` on failure

**Returns:** `ExecutionResult`
- `success` (boolean): Whether execution succeeded
- `error` (string | null): Error message if failed
- `result` (string | null): Base64-encoded output
- `mimeType` (string | null): Output MIME type
- `charset` (string | null): Output character encoding
- `binary` (boolean): Whether output is binary data
- `getString()`: Decode result as string
- `getBytes()`: Decode result as Buffer

#### `runStreaming(script, inputs?): AsyncGenerator<Buffer, StreamingResult>`

Execute a DataWeave script with streaming output.

```javascript
import { runStreaming } from 'dataweave-native';

const generator = runStreaming(
  '%dw 2.0\noutput application/json\n---\n[1, 2, 3, 4, 5]'
);

for await (const chunk of generator) {
  console.log('Chunk:', chunk.toString());
}

// Generator return value contains metadata:
const meta = await generator.return();
console.log('MIME type:', meta.value.mimeType);
```

**Parameters:**
- `script` (string): DataWeave script
- `inputs` (object, optional): Input variables

**Yields:** `Buffer` chunks as they're produced

**Returns:** `StreamingResult`
- `success` (boolean): Whether execution succeeded
- `error` (string | null): Error message if failed
- `mimeType` (string | null): Output MIME type
- `charset` (string | null): Output character encoding
- `binary` (boolean): Whether output is binary

#### `runTransform(script, input, opts?): AsyncGenerator<Buffer, StreamingResult>`

Execute a DataWeave script with streaming input and output (bidirectional streaming).

```javascript
import { runTransform } from 'dataweave-native';
import { createReadStream } from 'fs';

// Transform a large CSV file to JSON without loading it all into memory
const generator = runTransform(
  '%dw 2.0\noutput application/json\n---\npayload',
  createReadStream('large-file.csv'),
  {
    inputName: 'payload',
    mimeType: 'application/csv',
    charset: 'UTF-8',
    inputs: { threshold: 100 }
  }
);

for await (const chunk of generator) {
  process.stdout.write(chunk);
}
```

**Parameters:**
- `script` (string): DataWeave script
- `input` (AsyncIterable<Buffer> | Iterable<Buffer>): Streaming input data
- `opts` (object, optional): Options
  - `inputName` (string): Name of input variable (default: "payload")
  - `mimeType` (string): Input MIME type (default: "application/json")
  - `charset` (string | null): Input character encoding
  - `inputs` (object): Additional input variables

**Yields:** `Buffer` chunks as they're produced

**Returns:** `StreamingResult`

#### `cleanup(): void`

Clean up the global DataWeave runtime instance. Called automatically on process exit.

```javascript
import { cleanup } from 'dataweave-native';

// Manual cleanup (usually not needed)
cleanup();
```

### Class-Based API

For more control, use the `DataWeave` class directly:

```javascript
import { DataWeave } from 'dataweave-native';

const dw = new DataWeave();  // Optional: new DataWeave('/custom/path/to/dwlib.dylib')
dw.initialize();

try {
  const result = dw.run('2 + 2');
  console.log(result.getString());
} finally {
  dw.cleanup();
}
```

**Methods:**
- `initialize()`: Initialize the native library
- `cleanup()`: Release native resources
- `run(script, inputs?, opts?)`: Same as module-level `run()`
- `runStreaming(script, inputs?)`: Same as module-level `runStreaming()`
- `runTransform(script, input, opts?)`: Same as module-level `runTransform()`

### External Modules

DataWeave scripts can import external modules using the `resolveModule` option. The module-level convenience functions (`run()`, `runStreaming()`, `runTransform()`) operate on a lazily-initialized singleton that cannot be configured with a resolver — you must construct your own `DataWeave` instance:

```typescript
import { DataWeave, composeResolvers, modulesFromDirectory, modulesFromJars } from 'dataweave-native';

const dw = new DataWeave({
  resolveModule: composeResolvers(
    modulesFromDirectory('./my-modules'),
    await modulesFromJars(['./libs/dw-utils.jar'])
  )
});
dw.initialize();

const result = dw.run(`
  %dw 2.0
  import org::company::utils
  output application/json
  ---
  utils::doSomething()
`);

if (result.success) {
  console.log(result.getString());
}
```

See [docs/external-modules.md](docs/external-modules.md) for complete documentation, resolver factories, error handling, and dependency management. Note: a resolver runs with full process permissions (no sandboxing) — see the "Security / Trust Model" section there before pointing one at untrusted sources.

### Input Formats

Inputs can be provided in multiple formats:

#### Plain JavaScript Values

Automatically serialized to JSON:

```javascript
run('payload.name', { payload: { name: 'Alice', age: 30 } });
```

#### Explicit Input Configuration

For non-JSON inputs, use the input configuration format:

```javascript
const result = run(
  'payload.person.name',
  {
    payload: {
      content: Buffer.from('<?xml version="1.0"?><person><name>Bob</name></person>'),
      mimeType: 'application/xml',
      charset: 'UTF-8',
      properties: { nullValueOn: 'empty' }
    }
  }
);
```

**Input Configuration:**
- `content` (Buffer | string): Input data
- `mimeType` (string): MIME type (e.g., "application/xml", "application/csv")
- `charset` (string, optional): Character encoding
- `properties` (object, optional): Format-specific properties

#### CSV Example

```javascript
run(
  'payload.column_0[0]',
  {
    payload: {
      content: '123,456,789',
      mimeType: 'application/csv',
      properties: { header: false, separator: ',' }
    }
  }
);
```

## Examples

### JSON Transformation

```javascript
import { run } from 'dataweave-native';

const input = {
  users: [
    { id: 1, name: 'Alice', role: 'admin' },
    { id: 2, name: 'Bob', role: 'user' }
  ]
};

const script = `
%dw 2.0
output application/json
---
{
  admins: payload.users filter $.role == "admin" map $.name
}
`;

const result = run(script, { payload: input });
console.log(result.getString());
// {"admins":["Alice"]}
```

### XML Parsing

```javascript
import { run } from 'dataweave-native';

const xmlData = `
<?xml version="1.0"?>
<orders>
  <order><id>1</id><total>100</total></order>
  <order><id>2</id><total>200</total></order>
</orders>
`;

const script = `
%dw 2.0
output application/json
---
sum(payload.orders.*order.total)
`;

const result = run(script, {
  payload: {
    content: xmlData,
    mimeType: 'application/xml'
  }
});
console.log(result.getString());  // "300"
```

### Streaming Large Files

```javascript
import { runTransform } from 'dataweave-native';
import { createReadStream, createWriteStream } from 'fs';

const script = `
%dw 2.0
output application/json
---
payload filter $.amount > 1000
`;

const generator = runTransform(
  script,
  createReadStream('large-transactions.csv'),
  { mimeType: 'application/csv' }
);

const output = createWriteStream('filtered.json');

for await (const chunk of generator) {
  output.write(chunk);
}

output.end();
```

## Error Handling

### Result-Based Error Handling

```javascript
const result = run('invalid syntax');
if (!result.success) {
  console.error('Execution failed:', result.error);
  // Error: Unexpected token 'syntax'
}
```

### Exception-Based Error Handling

```javascript
import { run, DataWeaveScriptError } from 'dataweave-native';

try {
  run('invalid syntax', {}, { raiseOnError: true });
} catch (err) {
  if (err instanceof DataWeaveScriptError) {
    console.error('Script error:', err.message);
    console.error('Result:', err.result.error);
  }
}
```

### Streaming Error Handling

```javascript
try {
  const generator = runStreaming('invalid syntax');
  for await (const chunk of generator) {
    // Process chunks
  }
  const meta = await generator.return();
  if (!meta.value.success) {
    console.error('Streaming error:', meta.value.error);
  }
} catch (err) {
  console.error('Native error:', err);
}
```

## Threading Model

The Node.js binding uses **N-API** (Node-API) for C addon integration:

- **Thread-safe**: N-API calls are serialized on the Node.js event loop
- **Async operations**: Streaming operations yield control to the event loop between chunks
- **No blocking**: Long-running scripts execute on the native side without blocking the event loop

**Important:** Do not share a single `DataWeave` instance across Worker threads. Use the module-level functions (which use a global singleton) or create separate instances per thread.

**Custom module resolvers and Worker threads:** the native layer installs at
most one resolver callback for the whole process lifetime, and it is bound to
the Worker (main thread or a `worker_threads` Worker) that registered it
first — see [External Modules: Multiple Resolvers](docs/external-modules.md#multiple-resolvers-in-one-process).
Custom-module resolution attempted from any *other* thread is not routed to
that thread's own `resolveModule` callback; it silently falls back to
built-in modules only (custom module paths resolve as "not found" rather than
crashing or hanging). If you need per-Worker custom modules, resolve them on
the thread that first constructs a resolver-backed `DataWeave` instance, or
avoid resolver-backed instances in worker pools altogether.

## Platform Support

Published npm packages support:
- **macOS**: arm64 (M1/M2/M3)
- **Linux**: x86_64 (glibc 2.17+)
- **Windows**: x86_64

Source builds also support macOS x86_64.

The native library (`dwlib.dylib`/`.so`/`.dll`) must be built for your target platform.

## Troubleshooting

### "Cannot find module 'dwlib_addon.node'"

The N-API addon wasn't built. Run:

```bash
cd native-lib/node
npm run build:addon
```

### "Library not loaded: dwlib.dylib"

The native library isn't found. Options:

1. **Build it:** `./gradlew :native-lib:nativeCompile`
2. **Stage it:** `./gradlew :native-lib:stageNodeNativeLib`
3. **Set environment variable:** `export DATAWEAVE_NATIVE_LIB=/path/to/dwlib.dylib`

### "Error: Failed to initialize: SIGSEGV"

This should not happen with the N-API implementation. If you encounter this:

1. Ensure you're using Node >= 18
2. Verify the native library is compatible with your platform
3. Check for library version mismatches

### TypeScript Errors

The package includes full TypeScript definitions. If types aren't recognized:

```bash
npm install --save-dev @types/node
```

## Development

### Build from Source

```bash
# Build native library
./gradlew :native-lib:nativeCompile

# Stage native lib for Node.js
./gradlew :native-lib:stageNodeNativeLib

# Build addon and TypeScript
cd native-lib/node
npm install
npm run build
```

### Run Tests

```bash
cd native-lib/node
npm test
```

Tests use **Vitest** and cover:
- Basic execution
- Input handling (plain values, configured inputs)
- Streaming output
- Bidirectional streaming
- Error handling
- Concurrent execution

### Run Tests via Gradle

```bash
./gradlew :native-lib:nodeTest
```

## Performance

- **Buffered execution** (`run`): Best for small scripts with sub-MB outputs
- **Streaming execution** (`runStreaming`): Best for large outputs (MB+), reduces memory footprint
- **Bidirectional streaming** (`runTransform`): Best for large inputs and outputs, constant memory usage

Benchmark (1MB JSON transformation):
- `run()`: ~50ms, 2MB peak memory
- `runStreaming()`: ~55ms, 500KB peak memory
- `runTransform()`: ~60ms, 256KB peak memory (streaming input)

## See Also

- [Python Bindings](../python/README.md)
