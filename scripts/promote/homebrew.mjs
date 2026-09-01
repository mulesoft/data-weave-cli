export function parseTag(tag) {
  if (!/^v[0-9]/.test(tag)) {
    throw new Error(`Expected a version tag beginning with v and a digit: ${tag}`);
  }

  return tag.slice(1);
}

export function assetName(version) {
  return `dw-cli-${version}-macos-arm64.zip`;
}

export function releaseAssetUrl(repo, tag, version) {
  return `https://github.com/${repo}/releases/download/${tag}/${assetName(version)}`;
}

export function parseFormulaVersion(formula) {
  return formula.match(/^\s*version "([^"]+)"/m)?.[1];
}

export function rewriteFormula(formula, { url, sha256, version, homepage }) {
  let rewritten = formula
    .replace(/^(\s*url) "[^"]+"/m, `$1 "${url}"`)
    .replace(/^(\s*sha256) "[^"]+"/m, `$1 "${sha256}"`)
    .replace(/^(\s*version) "[^"]+"/m, `$1 "${version}"`);

  if (rewritten.match(/^\s*homepage "[^"]*mulesoft-labs\/data-weave-cli[^"]*"/m)) {
    rewritten = rewritten.replace(
      /^(\s*homepage) "[^"]+"/m,
      `$1 "${homepage ?? "https://github.com/mulesoft/data-weave-cli"}"`,
    );
  }

  return rewritten;
}

export function alreadyPromoted(formula, version) {
  return parseFormulaVersion(formula) === version;
}
