import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const source = readFileSync(join(__dirname, "..", "..", "src", "addon.c"), "utf-8");

function functionBody(name: string): string {
  const start = source.indexOf(`static napi_value ${name}`);
  expect(start, name).toBeGreaterThanOrEqual(0);
  const next = source.indexOf("\nstatic napi_value ", start + 1);
  return source.slice(start, next < 0 ? source.length : next);
}

describe("native addon N-API status contracts", () => {
  it("checks callback argument extraction at every production entrypoint", () => {
    for (const entrypoint of [
      "napi_run_script_streaming_engine",
      "napi_run_script_transform_engine",
      "napi_create_engine_with_resolver",
      "napi_destroy_engine",
      "napi_run_script_engine",
    ]) {
      const body = source.slice(source.indexOf(`static napi_value ${entrypoint}`));
      const call = body.slice(0, body.indexOf("napi_get_cb_info") + 300);
      expect(call, entrypoint).toMatch(/if \(napi_get_cb_info\([^\n]+\) != napi_ok/);
    }
  });

  it("checks result value creation before returning to JavaScript", () => {
    for (const entrypoint of ["napi_create_engine", "napi_create_engine_with_resolver"]) {
      const body = functionBody(entrypoint);
      expect(body, entrypoint).toMatch(/if \(napi_create_int64\([^\n]+\) != napi_ok\)/);
    }
    const runBody = functionBody("napi_run_script_engine");
    expect(runBody).toMatch(/napi_status value_status = result_copy/);
    expect(runBody).toMatch(/if \(value_status != napi_ok\)/);
  });
});
