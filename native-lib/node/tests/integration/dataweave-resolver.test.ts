import { describe, it, expect, afterAll } from "vitest";
import { DataWeave, cleanup } from '../../src/dataweave';
import { modulesFromMap } from '../../src/resolver';

afterAll(() => {
  cleanup();
});

describe('DataWeave with resolver', () => {
  it('resolves imported module from map', () => {
    const dw = new DataWeave({
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
});
