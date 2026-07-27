import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("result schema is valid JSON with the required shape", () => {
  const schema = JSON.parse(readFileSync(join(__dirname, "result.schema.json"), "utf-8"));
  assert.equal(schema.$schema, "http://json-schema.org/draft-07/schema#");
  const props = schema.properties;
  for (const key of ["schemaVersion", "runner", "env", "timestamp", "cases"]) {
    assert.ok(props[key], `schema must declare property ${key}`);
  }
  const envProps = props.env.properties;
  for (const key of ["os", "cpu", "runtimeVersion", "weaveVersion", "commit", "dwlibBuildId"]) {
    assert.ok(envProps[key], `env must declare ${key}`);
  }
  const metricEnum = props.cases.items.properties.metric.enum;
  assert.deepEqual(metricEnum.sort(), ["cold-start", "first-run", "streaming", "warm"]);
  const unitEnum = props.cases.items.properties.unit.enum;
  assert.deepEqual(unitEnum.sort(), ["MB/s", "ms"]);
});
