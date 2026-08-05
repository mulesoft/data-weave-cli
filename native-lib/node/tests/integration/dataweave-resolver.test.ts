import { describe, it, expect, afterAll } from "vitest";
import { DataWeave, cleanup } from '../../src/dataweave';
import { modulesFromMap } from '../../src/resolver';

afterAll(() => {
  cleanup();
});

// ScriptRuntime installs at most one resolver for the whole process lifetime
// (see ScriptRuntime.setResolver()): whichever DataWeave instance's resolver
// gets installed first "wins", and every later DataWeave instance in this
// file — regardless of its own resolveModule map — silently reuses it. Since
// vitest runs the `it` blocks in this file sequentially in the same process,
// that's always this first module-map, so it must contain every module path
// any test below needs to resolve for the first time (including the
// cross-thread regression test's two never-before-resolved paths).
const SHARED_RESOLVER_MODULES: Record<string, string> = {
  'org/test/lib.dwl': '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
  'org/test/resolverGuardInstall.dwl': '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
  'org/test/resolverGuardStreamed.dwl': '%dw 2.0\nfun greet(n: String) = "Hello " ++ n',
};

describe('DataWeave with resolver', () => {
  it('resolves imported module from map', () => {
    const dw = new DataWeave({
      resolveModule: modulesFromMap(SHARED_RESOLVER_MODULES),
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
    const dw = new DataWeave();  // No resolver
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
    const dw = new DataWeave({
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
    const dw = new DataWeave({
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

  // Regression test for the cross-thread resolver hazard: ScriptRuntime's engine
  // is a process-wide singleton, so once any .run() call installs a resolver on
  // it, that same composite resolver is used by ALL later execution paths --
  // including runStreaming()/runTransform(), whose native call executes on a
  // background uv_thread (see addon.c's streaming_thread_fn), not the JS thread
  // that registered the resolver. Before the thread-identity guard in addon.c's
  // resolve_module_callback, a streamed script importing a non-built-in module
  // would trigger a napi call from that background thread -- undefined behavior,
  // typically a crash of the whole process. After the guard, the callback fails
  // closed (reports "not found" instead of calling back into JS), so the script
  // fails cleanly with a compile error and the process survives.
  it('runStreaming fails cleanly (does not crash) for a custom module on the shared singleton engine', async () => {
    // Once a module name has been resolved anywhere in the process, the
    // DataWeave compiler caches it and won't call back into the resolver for
    // that same name again — so the install script and the streaming script
    // below import two module paths that no earlier test in this file has
    // imported yet (both pre-registered in SHARED_RESOLVER_MODULES above,
    // since only the first-installed resolver's map is ever consulted).
    const dw = new DataWeave({
      resolveModule: modulesFromMap(SHARED_RESOLVER_MODULES),
    });
    dw.initialize();

    // Install (or confirm already-installed) resolver on the shared singleton
    // engine via a synchronous run() call. Per ScriptRuntime.setResolver(), only
    // the first resolver registered for the process is ever used, so this is
    // safe to call even if an earlier test in this file already installed one.
    const installResult = dw.run(`
      %dw 2.0
      import org::test::resolverGuardInstall
      output application/json
      ---
      resolverGuardInstall::greet("Installer")
    `);
    expect(installResult.success).toBe(true);

    // Now stream a script that imports a DIFFERENT non-built-in module, never
    // resolved before in this process. The singleton engine's composite
    // resolver (ClassLoader + Callback) will miss in the ClassLoader half (not
    // a built-in) and fall through to the Callback half, invoking
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
