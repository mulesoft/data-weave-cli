# External Module Support

DataWeave scripts can import external modules using the `resolveModule` option. This allows you to organize code into reusable modules and import them into your scripts.

## Quick Start

```typescript
import { DataWeave, modulesFromMap } from 'dataweave-native';

const dw = new DataWeave({
  resolveModule: modulesFromMap({
    'org/company/lib.dwl': '%dw 2.0\nfun greet(n) = "Hello " ++ n',
  }),
});
dw.initialize();

const result = dw.run(`
  %dw 2.0
  import org::company::lib
  output application/json
  ---
  lib::greet("World")
`);
console.log(result.getString());  // "Hello World"
```

**Important:** The module-level convenience functions (`run()`, `runStreaming()`, `runTransform()` exported directly from `dataweave-native`) operate on a lazily-initialized singleton that takes no constructor options and therefore cannot be configured with `resolveModule` — you **must** construct your own `DataWeave` instance to use external modules, as shown above.

Additionally, external module resolution is currently supported only through `.run()` (the synchronous API). `.runStreaming()` and `.runTransform()` do not yet support external modules and will only have access to built-in modules.

## Resolver Factories

### modulesFromMap

In-memory map of module paths to source code:

```typescript
import { DataWeave, modulesFromMap } from 'dataweave-native';

const dw = new DataWeave({
  resolveModule: modulesFromMap({
    'org/test/lib.dwl': '%dw 2.0\nfun foo() = 42',
  }),
});
dw.initialize();
const result = dw.run('import org::test::lib\n%dw 2.0\n---\nlib::foo()');
```

Best for: Small, in-memory module sets; testing and development.

### modulesFromDirectory

Read modules from a directory tree on disk:

```typescript
import { DataWeave, modulesFromDirectory } from 'dataweave-native';

const dw = new DataWeave({
  resolveModule: modulesFromDirectory('./my-modules'),
});
dw.initialize();

// Resolves "org/test/lib.dwl" → reads "./my-modules/org/test/lib.dwl"
const result = dw.run('import org::test::lib\n%dw 2.0\n---\nlib::foo()');
```

Best for: Development and file-based module repositories.

### modulesFromJars

Extract modules from JAR files (asynchronous):

```typescript
import { DataWeave, modulesFromJars } from 'dataweave-native';

// modulesFromJars is async and returns a Promise
const resolver = await modulesFromJars([
  './libs/dw-strings-1.0.jar',
  './libs/dw-dates-2.1.jar',
]);

const dw = new DataWeave({
  resolveModule: resolver,
});
dw.initialize();
const result = dw.run('import org::mule::weave::core::Strings\n%dw 2.0\n---\nStrings::capitalize("hello")');
```

**Note:** `modulesFromJars()` returns a `Promise<ModuleResolver>` because JAR extraction must complete first. The returned resolver itself is synchronous and can be used repeatedly.

Best for: Packaged dependencies and distributed libraries.

### composeResolvers

Combine multiple resolvers with fallback chain (tries each in order, returns first match):

```typescript
import { DataWeave, composeResolvers, modulesFromMap, modulesFromDirectory, modulesFromJars } from 'dataweave-native';

const resolver = composeResolvers(
  modulesFromMap({ 'override.dwl': '...' }),       // Try first
  modulesFromDirectory('./shared'),                // Then directory
  await modulesFromJars(['./vendor/lib.jar'])      // Finally JAR
);

const dw = new DataWeave({
  resolveModule: resolver,
});
dw.initialize();
```

Best for: Layered resolution with fallbacks (overrides, shared libraries, vendor code).

## How It Works

- **One resolver per process**: The native engine maintains a single resolver per process lifetime. Only the first resolver registered is used; subsequent `DataWeave` instances with different resolvers will silently reuse the first one.
- **Resolution at compile time**: The resolver is invoked during script compilation, not per execution.
- **Synchronous resolution**: The resolver callback must be synchronous (no `async`/`await`, no Promise return).
- **Built-in modules**: Built-in modules (CompositeResolver) are always available and work alongside custom resolvers.

## Error Handling

### Module Not Found

When a module cannot be resolved:

```typescript
const dw = new DataWeave({
  resolveModule: modulesFromMap({
    // Only 'org/test/lib.dwl' is available
  }),
});
dw.initialize();

const result = dw.run(`
  %dw 2.0
  import org::missing::module  // Not found
  ---
  missing::something()
`);

if (!result.success) {
  console.error(result.error);  // "Unable to resolve module with identifier ..."
}
```

The resolver returns `null`, and the engine reports a compile-time error.

### File I/O Errors

When the resolver encounters file system errors (unreadable files, permission denied, etc.), the resolver throws an error. This error is caught internally by the native layer and the callback returns `null` — indistinguishable from "module not found" to the DataWeave compiler:

```typescript
const dw = new DataWeave({
  resolveModule: modulesFromDirectory('./my-modules'),
});
dw.initialize();

const result = dw.run(`
  %dw 2.0
  import org::test::lib
  ---
  lib::foo()
`);

if (!result.success) {
  // result.error is the same generic message as "module not found":
  console.error(result.error);  // "Unable to resolve module with identifier ..."
  // The actual error details (permissions, encoding, etc.) are not available
  // in the result object; see "Debugging" below for how to surface them.
}
```

**Debugging:** By default, a resolver failure logs only a fixed, content-free diagnostic line to stderr — the actual exception message and stack are suppressed, since they can carry resolver-controlled data (module source, credentials, filesystem paths). To see the detailed message and stack for diagnosing a failing resolver (e.g., directory does not exist, file unreadable due to permissions), set `DATAWEAVE_RESOLVER_DEBUG=1` in the process environment before running. Only enable this in a trusted debugging context, since the detailed output may expose sensitive resolver-controlled data.

### Multiple Resolvers in One Process

If you construct multiple `DataWeave` instances with different resolvers in the same process:

```typescript
const dw1 = new DataWeave({
  resolveModule: modulesFromMap({ 'a.dwl': '...' }),
});
dw1.initialize();

const dw2 = new DataWeave({
  resolveModule: modulesFromMap({ 'b.dwl': '...' }),
});
dw2.initialize();  // Only loads/ref-counts the native library — does NOT register a resolver

dw1.run('...');    // First resolver-backed run() in the process: installs dw1's resolver
dw2.run('...');    // Logs warning, silently reuses dw1's resolver instead of dw2's

// Both dw1 and dw2 use dw1's resolver (only 'a.dwl' is available)
```

**The rule is "first resolver-backed `run()` wins," not "first `initialize()` wins."**
`initialize()` only loads and ref-counts the native library; the resolver
itself is registered lazily, on whichever instance's `run()` executes first
with a resolver configured. If `dw2.run()` happens to execute before
`dw1.run()` — even though `dw1.initialize()` ran first — `dw2`'s resolver
wins instead.

**Workaround:** Use `composeResolvers()` to combine all modules into a single resolver:

```typescript
const resolver = composeResolvers(
  modulesFromMap({ 'a.dwl': '...' }),
  modulesFromMap({ 'b.dwl': '...' })
);

const dw1 = new DataWeave({ resolveModule: resolver });
dw1.initialize();

const dw2 = new DataWeave({ resolveModule: resolver });
dw2.initialize();  // Both use the same resolver
```

**Worker threads:** the same one-resolver-per-process rule applies across
`worker_threads` Workers, not just across instances on one thread. The
resolver callback is additionally bound to the specific thread that first
registered it. A resolver-backed `DataWeave` constructed and initialized on a
Worker other than the one that registered the process's resolver will not
have its `resolveModule` invoked at all — custom module paths resolve as "not
found" (falling back to built-ins only) rather than crashing. There is
currently no supported way to run distinct custom-module resolvers on
different Workers in the same process; either resolve modules on the thread
that owns the process's resolver, or avoid resolver-backed instances in
worker pools.

**Concurrent resolver-backed runs across Workers are unsupported and
memory-unsafe.** Beyond the "not found" fallback described above, calling a
resolver-backed `run()` concurrently from more than one Worker is not just
unsupported behavior — it is a memory-safety hazard. The native layer tracks
in-flight resolver results in unsynchronized, process-global state, and one
Worker's cleanup can free memory another Worker's concurrent call is still
using. Restrict resolver-backed execution to a single thread (or fully
serialize resolver-backed calls across Workers) until a future release
isolates per-instance engine state.

## Security / Trust Model

A `resolveModule` callback executes with **full process permissions** — the same trust model as the `dw` CLI resolving `.dwl` files from disk. There is no sandboxing: the callback can read/write the filesystem, make network calls, or run arbitrary Node.js code, and its return value (module source) is compiled and executed by the DataWeave engine with no additional isolation. Only configure a resolver that points at trusted sources (your own modules, vetted directories, or JARs from a trusted registry) — treat it with the same care you would give any code that runs with the permissions of your process.

**`modulesFromDirectory` and symlinks:** each lookup canonicalizes the candidate path and rejects it if the canonical path falls outside the configured base directory, which blocks a *stable* symlink pointing out of the module tree. This cannot portably close a time-of-check/time-of-use race, though: an actor able to write into the module tree could swap a validated file (or an ancestor directory) for an out-of-base symlink between the validation check and the subsequent read — Node has no portable equivalent of Linux's `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)` to close this atomically. As with the trust model above, only point `modulesFromDirectory` at a directory tree that is not writable by principals less trusted than the process itself.

## JAR Dependency Management

This release does NOT include Maven/coursier dependency resolution. You must provide JAR paths manually.

### Option 1: Download Using curl

```bash
curl -o dw-lib.jar https://repository.mulesoft.org/.../dw-lib-1.0.jar
```

### Option 2: Use Maven CLI

```bash
mvn dependency:copy \
  -Dartifact=org.mule.weave:dw-lib:1.0 \
  -DoutputDirectory=./libs
```

### Option 3: Use npm scripts (if configured)

Future releases may add `npm run dw-deps` for automatic resolution. Check your project's `package.json`.

Then pass JAR paths to `modulesFromJars()`:

```typescript
const resolver = await modulesFromJars([
  './libs/dw-lib-1.0.jar',
  './libs/dw-utils-2.1.jar'
]);

const dw = new DataWeave({ resolveModule: resolver });
dw.initialize();
```

## Complete Example

```typescript
import { DataWeave, composeResolvers, modulesFromMap, modulesFromDirectory, modulesFromJars } from 'dataweave-native';

async function main() {
  // Combine in-memory, file-based, and JAR-based modules
  const resolver = composeResolvers(
    // Override specific modules in-memory
    modulesFromMap({
      'org/company/constants.dwl': '%dw 2.0\nfun version() = "1.0.0"'
    }),
    // Share modules from local directory
    modulesFromDirectory('./shared-modules'),
    // Load dependencies from JARs
    await modulesFromJars(['./vendor/dw-strings.jar'])
  );

  const dw = new DataWeave({ resolveModule: resolver });
  dw.initialize();

  try {
    const result = dw.run(`
      %dw 2.0
      import org::company::constants
      import org::mule::weave::core::Strings
      output application/json
      ---
      {
        version: constants::version(),
        greeting: Strings::capitalize("hello world")
      }
    `);

    if (result.success) {
      console.log(result.getString());
      // {"version":"1.0.0","greeting":"Hello World"}
    } else {
      console.error('Error:', result.error);
    }
  } finally {
    dw.cleanup();
  }
}

main();
```
