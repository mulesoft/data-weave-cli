# Python Binding Modernization Design

> **Superseded in part (2026-09-15).** The architecture and streaming work in
> this historical design remains relevant, but its module-level convenience API
> was subsequently removed. Current public lifecycle semantics are defined by
> [`2026-09-15-explicit-instance-only-bindings-design.md`](2026-09-15-explicit-instance-only-bindings-design.md).

## Goal

Modernize the Python `dataweave` binding without changing its supported public
facade or the `dwlib` C ABI. The binding gains isolated test lanes, explicit
native lifecycle ownership, streaming support, and a master-only conformance
lane.

## Architecture

The `dataweave` package remains the stable facade. Public models and callback
types live in `models.py`; input/output wire conversion lives in `encoding.py`;
`native.py` owns ctypes library loading, isolate lifecycle, ABI signatures, and
native string release; `runtime.py` owns `DataWeave` orchestration.

`DataWeave` composes one `NativeRuntime`. This design originally retained
module-level functions with lazy singleton behavior; those functions were
subsequently removed. Callers use `DataWeave` as a context manager or manage its
lifecycle explicitly. Native failures raise `DataWeaveError`; script failures
remain result envelopes unless the caller selects `raise_on_error`.

## Streaming

Both output-only and input/output streaming use one bounded queue worker. Each
native worker attaches and detaches its own isolate thread. Callback exceptions
return `-1` and never unwind across the C ABI. Stream input retains remainders
when the iterable source provides chunks larger than the native buffer.

`Stream.close()` and its context manager request cancellation. This design
originally used a short bounded join and daemonized an unresponsive worker; that
cleanup behavior was subsequently superseded. Python cannot forcibly interrupt
a native call, so current explicit-instance `DataWeave.cleanup()` refuses while
an active registered streaming worker remains attached. Low-level callback input
larger than the supplied native buffer is rejected rather than truncated.

## Testing And TCK

Pytest has `unit`, `integration`, and `tck` lanes. Unit tests use fake native
collaborators; integration tests use staged `dwlib`; the TCK is on-demand and
master-only. The TCK comparator follows the Node policy, including structural
XML comparison with namespace declaration placement ignored while prefixes and
content remain significant.

TCK skips are reserved for concrete binding/environment capabilities. Known
runtime/output deviations are case-specific strict xfails, so new mismatches
and repaired baselines are visible failures. The one deferred-writer case runs
in a subprocess because its isolate teardown may block; the shared TCK runtime
remains managed and is cleaned up at session end.

## CI

The Python artifact action installs test dependencies, runs normal Python tests,
builds the wheel, and runs TCK on master. The staged TCK corpus is shared with
the Node lane. Generated native libraries, wheels, corpus files, and reports are
not committed.
