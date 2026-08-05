# External Module Support

DataWeave scripts can import external modules using the `resolveModule` option. This allows you to organize code into reusable modules and import them into your scripts.

## Quick Start

```typescript
import { DataWeave, modulesFromMap } from '@dataweave/native';

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

**Important:** External module resolution is currently supported only through `run()` (the synchronous, non-streaming API). `runStreaming()` and `runTransform()` do not yet support external modules and will only have access to built-in modules. To use external modules, construct a `DataWeave` instance directly with a `resolveModule` callback, call `.initialize()`, and use `.run()`.

## Resolver Factories

### modulesFromMap

In-memory map of module paths to source code:

```typescript
import { DataWeave, modulesFromMap } from '@dataweave/native';

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
import { DataWeave, modulesFromDirectory } from '@dataweave/native';

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
import { DataWeave, modulesFromJars } from '@dataweave/native';

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
import { DataWeave, composeResolvers, modulesFromMap, modulesFromDirectory, modulesFromJars } from '@dataweave/native';

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

When the resolver encounters file system errors:

```typescript
const dw = new DataWeave({
  resolveModule: modulesFromDirectory('./nonexistent-dir'),
});
dw.initialize();

// If a module exists in the path but can't be read (permissions, encoding, etc.),
// the resolver throws an Error with details
const result = dw.run(`
  %dw 2.0
  import org::test::lib
  ---
  lib::foo()
`);

if (!result.success) {
  console.error(result.error);  // File I/O error details
}
```

### Multiple DataWeave Instances

If you construct multiple `DataWeave` instances with different resolvers in the same process:

```typescript
const dw1 = new DataWeave({
  resolveModule: modulesFromMap({ 'a.dwl': '...' }),
});
dw1.initialize();

const dw2 = new DataWeave({
  resolveModule: modulesFromMap({ 'b.dwl': '...' }),
});
dw2.initialize();  // Logs warning, silently reuses dw1's resolver

// Both dw1 and dw2 use dw1's resolver (only 'a.dwl' is available)
```

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
import { DataWeave, composeResolvers, modulesFromMap, modulesFromDirectory, modulesFromJars } from '@dataweave/native';

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
