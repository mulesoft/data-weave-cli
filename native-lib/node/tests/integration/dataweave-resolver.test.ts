import { describe, it, expect, afterAll } from "vitest";
import { DataWeave, cleanup } from '../../src/dataweave';
import { modulesFromMap } from '../../src/resolver';

// Every test below constructs its own explicit DataWeave instance (rather
// than the module-level singleton) so each can configure its own resolver.
// `cleanup()` above only releases the *singleton* (`globalInstance`), which
// nothing in this file ever creates -- so without this tracking, every
// explicit instance's native library reference (and its own engine handle,
// see addon.c's create_engine/destroy_engine) would leak for the lifetime of
// the test process. Track every instance created in this file and release
// them all in afterAll.
const instances: DataWeave[] = [];
function trackedDataWeave(...args: ConstructorParameters<typeof DataWeave>): DataWeave {
  const dw = new DataWeave(...args);
  instances.push(dw);
  return dw;
}

afterAll(() => {
  for (const dw of instances) {
    dw.cleanup();
  }
  cleanup();
});

describe('DataWeave with resolver', () => {
  it('resolves imported module from map', () => {
    const dw = trackedDataWeave({
      resolveModule: modulesFromMap({
        'org/test/lib.dwl': '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
      }),
    });
    dw.initialize();

    const result = dw.run(`
      %dw 2.0
      import org::test::lib
      output application/json
      ---
      lib::greet("World")
    `);

    expect(result.success).toBe(true);
    expect(JSON.parse(result.getString()!)).toBe("Hello World");
  });

  it('works without resolver (backward compatible)', () => {
    const dw = trackedDataWeave();  // No resolver
    dw.initialize();

    const result = dw.run(`
      %dw 2.0
      output application/json
      ---
      { message: "no imports" }
    `);

    expect(result.success).toBe(true);
    expect(JSON.parse(result.getString()!)).toEqual({ message: "no imports" });
  });

  it('throws when module not found', () => {
    const dw = trackedDataWeave({
      resolveModule: modulesFromMap({ 'a.dwl': '...' }),
    });
    dw.initialize();

    expect(() => dw.run(`
      %dw 2.0
      import missing::mod
      output application/json
      ---
      {}
    `, undefined, { raiseOnError: true })).toThrow();
  });

  it('built-in modules still resolve with resolver', () => {
    const dw = trackedDataWeave({
      resolveModule: modulesFromMap({ 'custom.dwl': '...' }),
    });
    dw.initialize();

    // Built-in modules should still work (CompositeResolver: ClassLoader + Callback)
    const result = dw.run(`
      %dw 2.0
      import dw::core::Strings
      output application/json
      ---
      Strings::capitalize("hello")
    `);

    expect(result.success).toBe(true);
    expect(JSON.parse(result.getString()!)).toBe("Hello");
  });

  // Regression test for the cross-thread resolver hazard: each DataWeave
  // instance now owns its own native engine (see engine_bridge_t in addon.c),
  // but a resolver-backed engine's runStreaming()/runTransform() still
  // executes the native call on a background uv_thread (see addon.c's
  // streaming_thread_fn/transform_thread_fn), not the JS thread that created
  // the engine and its resolver bridge. resolve_module_callback detects that
  // thread-identity mismatch and fails closed (reports "not found" instead of
  // calling back into JS) rather than making an unsafe cross-thread napi
  // call, so the script fails cleanly with a compile error and the process
  // survives.
  it('runStreaming fails cleanly for a custom module on its own resolver-backed engine', async () => {
    const dw = trackedDataWeave({
      resolveModule: modulesFromMap({
        'org/test/resolverGuardStreamed.dwl': '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
      }),
    });
    dw.initialize();

    // Stream a script that imports a non-built-in module. This engine's
    // composite resolver (ClassLoader + Callback) misses in the ClassLoader
    // half (not a built-in) and falls through to the Callback half, invoking
    // resolve_module_callback from runStreaming's background thread.
    const chunks: Buffer[] = [];
    const gen = dw.runStreaming(`
      %dw 2.0
      import org::test::resolverGuardStreamed
      output application/json
      ---
      resolverGuardStreamed::greet("Streaming")
    `);
    let result = await gen.next();
    while (!result.done) {
      chunks.push(result.value);
      result = await gen.next();
    }
    const metadata = result.value;

    // Fails cleanly (built-ins-only fallback), rather than crashing the process.
    expect(metadata.success).toBe(false);
    expect(metadata.error).toBeTruthy();
    expect(chunks.length).toBe(0);
  });
});
