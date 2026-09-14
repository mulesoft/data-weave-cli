import { describe, it, expect, afterAll } from "vitest";
import { DataWeave, cleanup } from "../../src/dataweave";
import { modulesFromMap } from "../../src/resolver";

const instances: DataWeave[] = [];
function tracked(...args: ConstructorParameters<typeof DataWeave>): DataWeave {
  const dw = new DataWeave(...args);
  instances.push(dw);
  return dw;
}
afterAll(async () => {
  for (const dw of instances) await dw.cleanup();
  await cleanup();
});

const scriptImporting = (mod: string) =>
  `%dw 2.0\nimport org::test::${mod}\noutput application/json\n---\n${mod}::greet("X")`;

describe("independent engines (W-23692110)", () => {
  it("two instances resolve only their OWN module, with no cross-talk", () => {
    const dwA = tracked({ resolveModule: modulesFromMap({
      "org/test/a.dwl": '%dw 2.0\nfun greet(n: String) = "A:" ++ n' }) });
    const dwB = tracked({ resolveModule: modulesFromMap({
      "org/test/b.dwl": '%dw 2.0\nfun greet(n: String) = "B:" ++ n' }) });
    dwA.initialize();
    dwB.initialize();

    expect(JSON.parse(dwA.run(scriptImporting("a")).getString()!)).toBe("A:X");
    expect(JSON.parse(dwB.run(scriptImporting("b")).getString()!)).toBe("B:X");

    // Each engine misses the other's module.
    expect(dwA.run(scriptImporting("b")).success).toBe(false);
    expect(dwB.run(scriptImporting("a")).success).toBe(false);
  });

  it("built-in modules resolve in a resolver-backed engine", () => {
    const dw = tracked({ resolveModule: modulesFromMap({ "x.dwl": "..." }) });
    dw.initialize();
    const r = dw.run('%dw 2.0\nimport dw::core::Strings\noutput application/json\n---\nStrings::capitalize("hello")');
    expect(r.success).toBe(true);
    expect(JSON.parse(r.getString()!)).toBe("Hello");
  });

  // Carried forward from Task 3's review: runScriptEngine now returns "" (not
  // a thrown error) for a NULL native result, pushing error interpretation
  // entirely to parseNativeResponse() in this TS layer. A genuine script
  // error (as opposed to a NULL/empty native response) must still surface as
  // an ordinary unsuccessful ExecutionResult through the new handle-based
  // path -- not an unhandled parse exception or process crash.
  it("a genuine script error on a resolver-backed engine surfaces as success:false, not a throw", () => {
    const dw = tracked({ resolveModule: modulesFromMap({ "x.dwl": "..." }) });
    dw.initialize();

    let result: ReturnType<DataWeave["run"]> | undefined;
    expect(() => { result = dw.run("invalid_var_xyz"); }).not.toThrow();
    expect(result!.success).toBe(false);
    expect(result!.error).toBeTruthy();
  });

  // Confirms addon.c's argument-shifted runScriptStreamingEngine wiring (handle
  // as first argument, per Task 3) actually threads the handle through to a
  // real per-engine streaming run, not just the non-streaming run() path
  // exercised above. Uses a built-in import (not a custom resolver module):
  // runStreaming's native call executes on a background uv_thread whose
  // identity differs from the engine's owner thread, so a resolver-backed
  // engine fails closed for *custom* modules over streaming by design (see
  // dataweave-resolver.test.ts) -- that's not what this test is checking.
  it("runStreaming produces output on its own resolver-backed engine", async () => {
    const dw = tracked({ resolveModule: modulesFromMap({ "x.dwl": "..." }) });
    dw.initialize();

    const chunks: Buffer[] = [];
    const gen = dw.runStreaming(
      '%dw 2.0\nimport dw::core::Strings\noutput application/json\n---\nStrings::capitalize("stream")'
    );
    let result = await gen.next();
    while (!result.done) {
      chunks.push(result.value);
      result = await gen.next();
    }
    const metadata = result.value;

    expect(metadata.success).toBe(true);
    expect(JSON.parse(Buffer.concat(chunks).toString("utf-8"))).toBe("Stream");
  });

  // Confirms addon.c's argument-shifted runScriptTransformEngine wiring
  // likewise threads the handle through to a real per-engine transform run.
  it("runTransform produces output on its own resolver-backed engine", async () => {
    const dw = tracked({ resolveModule: modulesFromMap({ "x.dwl": "..." }) });
    dw.initialize();

    const inputData = [Buffer.from("[1, 2, 3]")];
    const script = "output application/json\n---\npayload map ($ * 10)";

    const chunks: Buffer[] = [];
    const gen = dw.runTransform(script, inputData, { mimeType: "application/json" });
    let result = await gen.next();
    while (!result.done) {
      chunks.push(result.value);
      result = await gen.next();
    }
    const metadata = result.value;

    expect(metadata.success).toBe(true);
    expect(JSON.parse(Buffer.concat(chunks).toString("utf-8"))).toEqual([10, 20, 30]);
  });
});
