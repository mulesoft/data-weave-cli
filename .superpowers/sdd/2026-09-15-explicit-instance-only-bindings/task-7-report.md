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

## Review Follow-Up

The four review findings were corrected on 2026-09-15:

- Node streaming, transform, and error examples in both binding READMEs now
  either construct, initialize, and clean an owned instance in `finally`, or are
  explicitly labeled as fragments inside an adjacent complete lifecycle.
- The Python modernization design marks bounded-join/daemonized-worker cleanup
  as superseded and records the current active-worker cleanup refusal.
- The multi-engine design labels Worker exit without cleanup as abnormal
  owner-env reclamation safety coverage, not an endorsed lifecycle.

### Exact Verification Commands and Output

```text
$ cd native-lib/python && python3 -m pytest tests/unit/test_ci_structure.py -q
..............                                                           [100%]
14 passed in 0.01s
```

```text
$ rg -n 'module-level (API|convenience)|global singleton|shared singleton|import \{ (run|runStreaming|runTransform|cleanup)|dataweave\.(run|run_streaming|run_transform|run_callback|run_input_output_callback|cleanup)\(' native-lib CLAUDE.md docs/superpowers/specs
docs/superpowers/specs/2026-08-24-python-module-resolver-design.md:55:- Configuring the module-level `dataweave.run()` singleton with a resolver.
docs/superpowers/specs/2026-08-24-python-module-resolver-design.md:114:This proposal originally left module-level convenience functions unchanged.
docs/superpowers/specs/2026-08-24-python-module-resolver-design.md:341:- module-level convenience API remained resolver-less in this proposal (it was
docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md:4:> this historical design remains relevant, but its module-level convenience API
native-lib/node/node-api-plan.md:4:> originally shipped module-level convenience functions. Those functions and
docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md:11:the Python and Node bindings. Remove the module-level convenience functions,
docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md:23:handle and may own a distinct custom module resolver. The module-level APIs sit
docs/superpowers/specs/2026-09-15-explicit-instance-only-bindings-design.md:223:current public lifecycle or promise an available module-level API must be
native-lib/python/tests/unit/test_ci_structure.py:214:        "dataweave.run(",
native-lib/python/tests/unit/test_ci_structure.py:215:        "dataweave.run_streaming(",
native-lib/python/tests/unit/test_ci_structure.py:216:        "dataweave.run_transform(",
native-lib/python/tests/unit/test_ci_structure.py:217:        "dataweave.run_callback(",
native-lib/python/tests/unit/test_ci_structure.py:218:        "dataweave.run_input_output_callback(",
native-lib/python/tests/unit/test_ci_structure.py:219:        "dataweave.cleanup(",
native-lib/python/tests/unit/test_ci_structure.py:220:        "import { runTransform, cleanup }",
```

Every focused stale-guidance result above was manually inspected. The design and
plan results explicitly describe prior or removed behavior; the test results are
negative literals that reject obsolete example usage. There are no current
runnable-example or present-tense API-guidance matches.

```text
$ rg -n 'short bounded join|daemonized|Worker exit without|normal Worker exit without|without `cleanup\(\)`' native-lib/README.md native-lib/node/README.md docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md
docs/superpowers/specs/2026-08-19-python-binding-modernization-design.md:36:originally used a short bounded join and daemonized an unresponsive worker; that
docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md:138:concurrent creation, execution, abandonment (env death without `cleanup()`), and teardown across
docs/superpowers/specs/2026-08-07-native-lib-multi-engine-design.md:718:  reclamation when a Worker exits without `cleanup()`** (resource-safety coverage for the
```

These three results are explicitly historical or describe abnormal native
owner-env reclamation. None recommends skipping deterministic cleanup.

```text
$ git diff --check
```

No output; the whitespace check passed.
