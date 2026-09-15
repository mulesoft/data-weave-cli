# Task 7 Report: Rewrite Binding and Architecture Documentation

## Outcome

- Rewrote Python and Node first-use, buffered, streaming, transform, callback,
  resolver, error, Worker, signal, and cleanup guidance around caller-owned
  `DataWeave` instances.
- Preserved explicit-instance and native lifecycle hardening, including shared
  isolate reference counting, cleanup retry/leak contracts, callback safety,
  stream admission, and asynchronous Node cleanup.
- Removed current guidance for hidden singleton engines, module cleanup,
  singleton generations, and automatic Python/Node process hooks.
- Added dated supersession notes to historical Python and Node designs and
  updated the consolidated multi-engine final-state maps without erasing the
  historical Java/C singleton rationale.
- Reviewed the controller-authored explicit-instance design for consistency and
  preserved its approved semantics unchanged.
- Updated the Python README resolver contract test and its negation case while
  preserving Task 5's example static and cleanup-order tests.

## Stale-Guidance Review

The required focused search has no current runnable-example or present-tense API
guidance matches. Remaining matches were inspected and retained only when they
are:

- explicitly historical or supersession prose;
- the approved design's description of the removed API;
- negative literals in `test_examples_do_not_use_removed_module_level_runtime_api`;
  or
- DataWeave language `runtime/module-singleton-out.json` fixture terminology.

Lower-level module-scoped shared-isolate/ref-count state remains documented and
is explicitly distinguished from a high-level execution singleton.

## Verification

```text
cd native-lib/python && python3 -m pytest tests/unit/test_ci_structure.py -q
14 passed in 0.01s
```

`git diff --check` passed. Status, full task diff, diff statistics, and the last
10 commits were inspected before staging and commit.
