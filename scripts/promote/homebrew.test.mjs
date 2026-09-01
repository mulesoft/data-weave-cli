import { test } from "node:test";
import assert from "node:assert/strict";
import {
  parseTag,
  assetName,
  releaseAssetUrl,
  parseFormulaVersion,
  rewriteFormula,
  alreadyPromoted,
} from "./homebrew.mjs";

const SAMPLE = `class Dw < Formula
  desc "DataWeave CLI"
  homepage "https://github.com/mulesoft-labs/data-weave-cli"
  url "https://github.com/mulesoft-labs/data-weave-cli/releases/download/v1.0.36/dw-1.0.36-macOS"
  sha256 "d503f000c24bf0a7701df917561b930bccfc98a922b6425065e13c93f73831fe"
  version "2.11.0-20251026"

  def install
    prefix.install "bin"
    prefix.install "libs"
  end
end
`;

test("parseTag strips v", () => {
  assert.equal(parseTag("v1.2.3"), "1.2.3");
  assert.throws(() => parseTag("1.2.3"));
  assert.throws(() => parseTag("main"));
});

test("asset names and urls", () => {
  assert.equal(assetName("1.2.3"), "dw-cli-1.2.3-macos-arm64.zip");
  assert.equal(
    releaseAssetUrl("mulesoft/data-weave-cli", "v1.2.3", "1.2.3"),
    "https://github.com/mulesoft/data-weave-cli/releases/download/v1.2.3/dw-cli-1.2.3-macos-arm64.zip",
  );
});

test("parse and alreadyPromoted", () => {
  assert.equal(parseFormulaVersion(SAMPLE), "2.11.0-20251026");
  assert.equal(alreadyPromoted(SAMPLE, "2.11.0-20251026"), true);
  assert.equal(alreadyPromoted(SAMPLE, "1.2.3"), false);
});

test("rewriteFormula updates url sha version homepage", () => {
  const next = rewriteFormula(SAMPLE, {
    url: "https://github.com/mulesoft/data-weave-cli/releases/download/v1.2.3/dw-cli-1.2.3-macos-arm64.zip",
    sha256: "abc",
    version: "1.2.3",
  });
  assert.match(
    next,
    /url "https:\/\/github.com\/mulesoft\/data-weave-cli\/releases\/download\/v1.2.3\/dw-cli-1.2.3-macos-arm64.zip"/,
  );
  assert.match(next, /sha256 "abc"/);
  assert.match(next, /version "1.2.3"/);
  assert.match(next, /homepage "https:\/\/github.com\/mulesoft\/data-weave-cli"/);
  assert.match(next, /prefix.install "bin"/);
});
