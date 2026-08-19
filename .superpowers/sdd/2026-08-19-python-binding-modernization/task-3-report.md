# Task 3 Report: Extract Models And Encoding

## Files Changed

- Added `native-lib/python/src/dataweave/models.py` for public models, exceptions, callback aliases, ctypes callback signatures, and `Stream`.
- Added `native-lib/python/src/dataweave/encoding.py` for input normalization and native response parsing.
- Updated `native-lib/python/src/dataweave/__init__.py` to re-export the legacy public API and delegate internal model/encoding operations to the new modules.
- Updated `native-lib/python/tests/unit/test_facade.py`, `test_models.py`, and `test_encoding.py` with facade/export compatibility coverage.
- Updated `native-lib/python/pytest.ini` to use pytest importlib import mode, avoiding same-basename test-module collection collisions between unit and integration lanes.

## Behavior Preservation

- `dataweave.__all__`, module-level public names, and existing call signatures remain unchanged.
- `ExecutionResult.get_bytes()` and `ExecutionResult.get_string()` retain their prior base64, binary, charset, and unsuccessful-result behavior.
- The public models, exceptions, callback type aliases, and ctypes callback signatures are now also available from `dataweave.models`.
- `normalize_input_value`, `parse_native_encoded_response`, and `parse_streaming_result` are now public from `dataweave.encoding`; legacy private facade aliases remain for existing internal callers and tests.
- Native wire keys remain unchanged: `mimeType`, `charset`, `binary`, `result`, and `error`.
- No native/runtime extraction was performed.

## Verification

Command:

```bash
cd native-lib/python
python3 -m pytest tests/unit tests/integration -m "unit or integration" -v
```

Output:

```text
49 passed in 1.46s
```

Additional check:

```bash
git diff --check
```

Output: no whitespace errors.

## Commit

`1276d12 refactor: separate Python binding models and encoding`

## Concerns

- The test tree has same-basename unit and integration modules (`test_streaming.py`). Pytest’s default prepend import mode causes collection to fail; `--import-mode=importlib` is now configured so the required combined test command runs consistently.

## Fix Round 1

- Replaced the dynamic facade export assertion with a fixed, pre-Task-3 list of all 18 legacy public names.
- The test now verifies every legacy name is explicitly present in `dataweave.__all__` and resolves through `getattr(dataweave, name)`.
- Focused verification: `python3 -m pytest tests/unit/test_facade.py -m unit -v` reported `3 passed in 0.01s`.
- Combined verification and fix-round commit are recorded in the follow-up commit for this round.
