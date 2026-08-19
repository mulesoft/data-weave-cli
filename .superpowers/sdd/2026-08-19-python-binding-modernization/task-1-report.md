# Task 1 Report: Pytest Lanes And Existing Behavior

## Files Changed

- `native-lib/python/pytest.ini`: registers `unit`, `integration`, and `tck` markers; excludes TCK tests by default.
- `native-lib/python/pyproject.toml`: declares the `test` optional dependency group with `pytest` and `pytest-cov`.
- `native-lib/python/tests/conftest.py`: adds source-path setup, automatic module-global runtime cleanup, and a streaming collection fixture.
- `native-lib/python/tests/integration/test_execution.py`: migrates five execution and input-conversion scenarios.
- `native-lib/python/tests/integration/test_streaming.py`: migrates eight streaming and transform scenarios.
- `native-lib/python/tests/integration/test_callbacks.py`: migrates four callback streaming scenarios.
- `native-lib/python/tests/integration/test_lifecycle.py`: migrates the explicit lifecycle scenario.
- `native-lib/python/tests/test_dataweave_module.py`: removed the superseded hand-run script.
- `native-lib/build.gradle`: makes `pythonTest` run normal pytest lanes and write JUnit plus coverage XML under `native-lib/build`.

## Design Choices

- Retained all 18 legacy scenarios as separately named `@pytest.mark.integration` tests, grouped by execution mode.
- Used an autouse fixture to call `dataweave.cleanup()` before and after every test, isolating the module-level native runtime without changing public runtime behavior or the C ABI.
- Kept TCK excluded from default pytest collection and made Gradle explicitly run only `unit or integration` markers.
- Wrote test reports to `native-lib/build/test-results/python/junit.xml` and coverage to `native-lib/build/reports/coverage/python/coverage.xml`.

## Tests Run

1. `/var/folders/n2/069kfxz14k3dg0dctt0gblm80000gn/T/opencode/dataweave-python-test/bin/python -m pip install '.[test]'`
   - Passed. Installed `pytest 8.4.2` and `pytest-cov 7.1.0` in an isolated temporary virtual environment.
2. `/var/folders/n2/069kfxz14k3dg0dctt0gblm80000gn/T/opencode/dataweave-python-test/bin/python -m pytest tests/integration -m integration -v`
   - Passed: `18 passed in 0.39s`.
3. `./gradlew native-lib:pythonTest -PpythonExe=/var/folders/n2/069kfxz14k3dg0dctt0gblm80000gn/T/opencode/dataweave-python-test/bin/python`
   - Passed: native `dwlib` compiled and staged; pytest reported `18 passed in 1.04s`; JUnit and coverage XML were written to the configured build paths.
4. `pytest -m 'not tck' --collect-only -q` using the temporary virtual environment
   - Passed: 18 tests collected, confirming default TCK exclusion.
5. `git diff --check`
   - Passed.

## Commit

- `9cf376d test: migrate Python binding checks to pytest`

## Concerns

- The system Python does not have pytest and cannot write its global site-packages. Verification used an isolated temporary virtual environment supplied through Gradle's existing `-PpythonExe` override.
- Native-image and Gradle emit existing Java/native-image deprecation warnings during the native build; the task itself completed successfully.
