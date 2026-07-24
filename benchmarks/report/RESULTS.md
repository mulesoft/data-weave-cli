# DataWeave benchmark results

_Generated from commit `0cd78a1` on 2026-07-24T14:33:03.805941Z._

**Environment** (Apple M4 Max, Mac OS X-aarch64):  
`engine` — jvm 17.0.16, weave 2.12.0-20260413  
`node-wrapper` — node v22.23.0, weave 2.12.0-20260413  
`python-wrapper` — python 3.9.6, weave 2.12.0-20260413

> Indicative only — timings are from a single run on one machine, not a dedicated bench box.

## Table

| case | metric | unit | engine | node-wrapper | python-wrapper | Δ vs engine |
| --- | --- | --- | --- | --- | --- | --- |
| trivial | cold-start | ms | 148.72 | 38.44 | 39.33 | -74.1% |
| trivial | first-run | ms | 324.17 | 8.34 | 8.59 | -97.4% |
| trivial | warm | ms | 0.10 | 0.08 | 0.08 | -17.4% |
| object-transform | first-run | ms | 635.23 | 16.30 | 16.80 | -97.4% |
| object-transform | warm | ms | 0.28 | 0.23 | 0.22 | -18.1% |
| map-scale | first-run | ms | 764.13 | 140.45 | 144.91 | -81.6% |
| map-scale | warm | ms | 35.58 | 117.66 | 117.99 | +230.6% |
| map-scale | streaming | MB/s | 59.03 | 29.71 | 29.56 | n/a |
| xml-to-csv | first-run | ms | 337.29 | 8.63 | 9.48 | -97.4% |
| xml-to-csv | warm | ms | 0.10 | 0.13 | 0.09 | +29.9% |
| json-stream | first-run | ms | 717.85 | 122.23 | 127.18 | -83.0% |
| json-stream | warm | ms | 26.62 | 99.34 | 100.08 | +273.1% |
| json-stream | streaming | MB/s | 77.89 | 37.92 | 37.83 | n/a |
| compile-heavy | first-run | ms | 646.98 | 17.02 | 18.00 | -97.4% |
| compile-heavy | warm | ms | 1.02 | 0.84 | 1.06 | -17.9% |
| csv-to-json | first-run | ms | 616.73 | 16.28 | 17.50 | -97.4% |
| csv-to-json | warm | ms | 0.16 | 0.18 | 0.17 | +15.7% |
| xml-to-json | first-run | ms | 628.10 | 16.23 | 17.23 | -97.4% |
| xml-to-json | warm | ms | 0.17 | 0.18 | 0.17 | +6.5% |
| deep-selector | first-run | ms | 323.61 | 8.64 | 8.77 | -97.3% |
| deep-selector | warm | ms | 0.08 | 0.11 | 0.10 | +33.4% |
| group-by | first-run | ms | 842.98 | 193.86 | 197.31 | -77.0% |
| group-by | warm | ms | 71.51 | 171.33 | 169.72 | +139.6% |

> `n/a` deltas mark metrics that are not like-for-like across runners: the engine's `streaming` times a full compile+write of the whole input per iteration, while native-lib runners time an incrementally-chunked transform. Compare each runner's absolute `streaming` throughput, not the delta.

## Charts

One chart per corpus case, one bar per runner (`engine`, `node-wrapper`, `python-wrapper`). A case's metrics differ in unit and scale, so each metric is a separate single-unit chart. `engine` is the table's delta baseline.

### trivial

```mermaid
xychart-beta
    title "trivial — cold-start (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [148.722, 38.445, 39.327]
```

```mermaid
xychart-beta
    title "trivial — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [324.171, 8.337, 8.589]
```

```mermaid
xychart-beta
    title "trivial — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.101, 0.084, 0.077]
```

### object-transform

```mermaid
xychart-beta
    title "object-transform — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [635.227, 16.301, 16.803]
```

```mermaid
xychart-beta
    title "object-transform — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.279, 0.229, 0.218]
```

### map-scale

```mermaid
xychart-beta
    title "map-scale — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [764.132, 140.449, 144.906]
```

```mermaid
xychart-beta
    title "map-scale — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [35.585, 117.659, 117.985]
```

```mermaid
xychart-beta
    title "map-scale — streaming (MB/s, higher is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "MB/s"
    bar [59.028, 29.715, 29.557]
```

### xml-to-csv

```mermaid
xychart-beta
    title "xml-to-csv — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [337.29, 8.633, 9.483]
```

```mermaid
xychart-beta
    title "xml-to-csv — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.098, 0.127, 0.093]
```

### json-stream

```mermaid
xychart-beta
    title "json-stream — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [717.852, 122.225, 127.181]
```

```mermaid
xychart-beta
    title "json-stream — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [26.624, 99.339, 100.085]
```

```mermaid
xychart-beta
    title "json-stream — streaming (MB/s, higher is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "MB/s"
    bar [77.89, 37.923, 37.832]
```

### compile-heavy

```mermaid
xychart-beta
    title "compile-heavy — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [646.983, 17.016, 17.998]
```

```mermaid
xychart-beta
    title "compile-heavy — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [1.022, 0.839, 1.057]
```

### csv-to-json

```mermaid
xychart-beta
    title "csv-to-json — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [616.727, 16.284, 17.503]
```

```mermaid
xychart-beta
    title "csv-to-json — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.16, 0.185, 0.167]
```

### xml-to-json

```mermaid
xychart-beta
    title "xml-to-json — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [628.095, 16.235, 17.228]
```

```mermaid
xychart-beta
    title "xml-to-json — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.173, 0.184, 0.17]
```

### deep-selector

```mermaid
xychart-beta
    title "deep-selector — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [323.607, 8.642, 8.766]
```

```mermaid
xychart-beta
    title "deep-selector — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.084, 0.112, 0.099]
```

### group-by

```mermaid
xychart-beta
    title "group-by — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [842.983, 193.864, 197.309]
```

```mermaid
xychart-beta
    title "group-by — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [71.505, 171.332, 169.718]
```
