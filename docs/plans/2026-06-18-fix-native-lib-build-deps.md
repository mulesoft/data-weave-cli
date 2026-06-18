# Fix Native-Lib Build Dependencies

## Problem

The `:native-lib:goTest` and `:native-lib:rustTest` tasks can fail when run after `clean` because Gradle's incremental build system does not properly track that these tasks depend on the output of `nativeCompile` (specifically `graal_isolate.h` and `dwlib.*`).

While `dependsOn` ensures execution order, Gradle's `inputs`/`outputs` declarations are needed to:
1. Ensure proper up-to-date checking
2. Enable `--parallel` execution without races
3. Make the dependency graph explicit for build caching

## Solution

### 1. Declare `nativeCompile` outputs

Both in `build.gradle` (root, for all subprojects) and `native-lib/build.gradle`:

```groovy
tasks.matching { it.name == 'nativeCompile' }.configureEach { t ->
    t.outputs.dir("${buildDir}/native/nativeCompile")
}
```

### 2. Declare `inputs` on consumer tasks

In `native-lib/build.gradle`, `goTest` and `rustTest`:

```groovy
inputs.dir("${buildDir}/native/nativeCompile")
```

### 3. Ensure `build` depends on `test`

```groovy
tasks.named('build') {
    dependsOn tasks.named('test')
}
```

### 4. Documentation

Created `BUILDING.md` with full build prerequisites, instructions, and troubleshooting.

## Validation

```bash
./gradlew clean :native-lib:goTest    # Previously failed, now succeeds
./gradlew clean build                  # Full build with all tests
```
