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
    const source = modules[modulePath];

    if (source === undefined) {
      console.debug(`Module not found in map: ${modulePath}`);
      return null;
    }

    return source;
  };
}
