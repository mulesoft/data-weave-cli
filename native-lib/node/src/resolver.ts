import * as fs from "fs";
import * as path from "path";
import AdmZip from "adm-zip";

/**
 * Module resolver function type.
 * Takes a module path (e.g., "org/mule/weave/v2/libs/lib.dwl") and returns
 * the .dwl source as a string, or null if not found.
 *
 * MUST be synchronous (no async/await, no Promise return).
 */
export type ModuleResolver = (modulePath: string) => string | null;

/**
 * Creates a resolver backed by an in-memory map of path -> source.
 *
 * @param modules Map of module paths to .dwl source
 * @returns Resolver function
 *
 * @example
 * const resolver = modulesFromMap({
 *   'org/test/lib.dwl': '%dw 2.0\nfun greet(n) = "Hello " ++ n'
 * });
 */
export function modulesFromMap(modules: Record<string, string>): ModuleResolver {
  return (modulePath: string): string | null => {
    // Object.hasOwn (not `in`) avoids matching inherited properties like
    // "toString" or "constructor", which would violate the string | null
    // contract above.
    if (Object.hasOwn(modules, modulePath)) {
      return modules[modulePath];
    }
    return null;
  };
}

/**
 * True iff `candidate` is `base` itself or lies strictly beneath it.
 *
 * Deliberately not a `startsWith(base + path.sep)` string check: that
 * degrades at a root base (`/` -> `startsWith("//")`, and the analogous
 * duplicated separator on Windows drive/UNC roots), which would reject every
 * legitimate child of a root-level baseDir. path.relative() has no such edge
 * case: a candidate outside base always relates back via a leading "..", or
 * — for a different drive/root on Windows — comes back absolute.
 */
function isContained(base: string, candidate: string): boolean {
  if (candidate === base) return true;
  const rel = path.relative(base, candidate);
  // An escape climbs out via a ".." segment (rel === ".." or rel starts with
  // "../"). A literal filename that merely starts with two dots, e.g.
  // "..foo.dwl", is a single segment and is NOT an escape — checking for the
  // separator (or an exact ".." match) avoids misclassifying it.
  const isUpwardEscape = rel === ".." || rel.startsWith(".." + path.sep);
  return rel !== "" && !isUpwardEscape && !path.isAbsolute(rel);
}

/**
 * Creates a resolver that reads .dwl files from a directory tree.
 * Scans recursively for nested namespace structures.
 * Reads from disk on every resolution (no caching).
 *
 * @param baseDir Base directory to scan for .dwl files
 * @returns Resolver function
 *
 * @example
 * const resolver = modulesFromDirectory('./my-modules');
 * // Resolves "org/test/lib.dwl" → reads "./my-modules/org/test/lib.dwl"
 */
export function modulesFromDirectory(baseDir: string): ModuleResolver {
  // Unresolved base, for the cheap lexical check below (must be compared
  // against an equally-unresolved candidate path — see baseDirResolved).
  const baseDirLexical = path.resolve(baseDir);
  // Canonicalized base, for the filesystem-truth check below. This also
  // fails fast if baseDir doesn't exist, rather than silently resolving
  // nothing. Kept separate from baseDirLexical: on macOS, os.tmpdir() (and
  // other paths) can live under a symlink (e.g. /var -> /private/var), so
  // comparing an unresolved candidate against a canonicalized base would
  // reject every legitimate path.
  const baseDirResolved = fs.realpathSync(baseDir);

  return (modulePath: string): string | null => {
    // Join against the already-absolute baseDirLexical, not the original
    // (possibly relative) baseDir: a relative baseDir re-resolves against the
    // *current* cwd on every call, so a later process.chdir() would silently
    // relocate every lookup. baseDirLexical was captured once, above, so it
    // is stable regardless of later chdir() calls.
    const fullPath = path.resolve(path.join(baseDirLexical, modulePath));

    // Lexical containment check first (cheap, catches plain ".." traversal
    // before touching the filesystem). Compared against the unresolved base
    // so a symlinked baseDir itself doesn't cause a false rejection.
    if (!isContained(baseDirLexical, fullPath)) {
      return null; // Path escapes baseDir
    }

    let realPath: string;
    try {
      // Canonicalize the candidate too: a symlink inside baseDir can point
      // outside it and would otherwise pass the lexical check above, since
      // path.resolve() never touches the filesystem.
      realPath = fs.realpathSync(fullPath);
    } catch (error) {
      if (error instanceof Error && "code" in error && (error as NodeJS.ErrnoException).code === "ENOENT") {
        return null;
      }
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to read module ${modulePath} from ${fullPath}: ${message}`);
    }

    // Re-check containment against the canonical path, rejecting symlinks
    // (or symlinked ancestor directories) that resolve outside baseDir.
    if (!isContained(baseDirResolved, realPath)) {
      return null;
    }

    try {
      return fs.readFileSync(realPath, "utf-8");
    } catch (error) {
      // File not found is expected, return null
      if (error instanceof Error && "code" in error && (error as NodeJS.ErrnoException).code === "ENOENT") {
        return null;
      }
      // Other errors (permissions, invalid UTF-8, etc.) should throw
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to read module ${modulePath} from ${fullPath}: ${message}`);
    }
  };
}

/**
 * Creates a resolver that extracts .dwl files from JAR archives.
 * Returns a Promise because JAR extraction must complete before resolver is used.
 * The returned resolver itself is synchronous (backed by in-memory map).
 *
 * @param jarPaths Array of paths to JAR files
 * @returns Promise resolving to resolver function
 *
 * @example
 * const resolver = await modulesFromJars(['./libs/dw-strings.jar']);
 * // Now use synchronously: resolver('dw/core/Strings.dwl')
 */
export async function modulesFromJars(jarPaths: string[]): Promise<ModuleResolver> {
  const modules: Record<string, string> = {};

  for (const jarPath of jarPaths) {
    try {
      const zip = new AdmZip(jarPath);
      const entries = zip.getEntries();

      for (const entry of entries) {
        // Extract only .dwl files, skip directories
        if (!entry.isDirectory && entry.entryName.endsWith(".dwl")) {
          const source = entry.getData().toString("utf-8");
          modules[entry.entryName] = source;
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to read JAR ${jarPath}: ${message}`);
    }
  }

  // Return synchronous resolver backed by extracted modules
  return modulesFromMap(modules);
}

/**
 * Composes multiple resolvers into one with fallback chain.
 * Tries each resolver in order, returns first non-null result.
 *
 * @param resolvers Resolvers to try in order
 * @returns Composite resolver function
 *
 * @example
 * const resolver = composeResolvers(
 *   modulesFromMap({ 'override.dwl': '...' }),  // Try first
 *   modulesFromDirectory('./shared'),            // Then directory
 *   await modulesFromJars(['./vendor/lib.jar'])  // Finally JAR
 * );
 */
export function composeResolvers(...resolvers: ModuleResolver[]): ModuleResolver {
  return (modulePath: string): string | null => {
    for (const resolver of resolvers) {
      const result = resolver(modulePath);
      if (result !== null) {
        return result; // First match wins
      }
    }

    // None matched
    console.debug(`Module not found in any resolver: ${modulePath}`);
    return null;
  };
}
