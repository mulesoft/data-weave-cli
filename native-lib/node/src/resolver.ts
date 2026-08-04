import * as fs from "fs";
import * as path from "path";

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
    if (modulePath in modules) {
      return modules[modulePath];
    }
    return null;
  };
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
  return (modulePath: string): string | null => {
    const fullPath = path.resolve(path.join(baseDir, modulePath));
    const baseDirResolved = path.resolve(baseDir);

    // Prevent path traversal - ensure resolved path is within baseDir
    if (!fullPath.startsWith(baseDirResolved + path.sep) && fullPath !== baseDirResolved) {
      return null; // Path escapes baseDir
    }

    try {
      return fs.readFileSync(fullPath, "utf-8");
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
