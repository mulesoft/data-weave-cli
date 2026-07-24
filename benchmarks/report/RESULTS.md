# DataWeave benchmark results

_Generated from commit `d6611d4` on 2026-07-24T19:27:26.294Z._

**Environment** (Apple M4 Max, Mac OS X-aarch64):  
`engine` — jvm 17.0.16, weave 2.12.0-20260413  
`node-wrapper` — node v22.23.0, weave 2.12.0-20260413  
`python-wrapper` — python 3.9.6, weave 2.12.0-20260413

> Indicative only — timings are from a single run on one machine, not a dedicated bench box.

## Table

| case | metric | unit | engine | node-wrapper | python-wrapper | Δ vs engine |
| --- | --- | --- | --- | --- | --- | --- |
| trivial | cold-start | ms | 149.48 | 38.14 | 42.67 | -74.5% |
| trivial | first-run | ms | 309.21 | 8.53 | 8.78 | -97.2% |
| trivial | warm | ms | 0.10 | 0.09 | 0.08 | -11.0% |
| object-transform | first-run | ms | 583.65 | 16.73 | 17.01 | -97.1% |
| object-transform | warm | ms | 0.26 | 0.24 | 0.22 | -9.8% |
| map-scale | first-run | ms | 700.98 | 145.62 | 146.48 | -79.2% |
| map-scale | warm | ms | 33.50 | 117.51 | 118.96 | +250.7% |
| map-scale | streaming | MB/s | 52.43 | 28.53 | 29.66 | -45.6% |
| xml-to-csv | first-run | ms | 323.17 | 8.62 | 8.83 | -97.3% |
| xml-to-csv | warm | ms | 0.10 | 0.11 | 0.09 | +4.2% |
| json-stream | first-run | ms | 673.21 | 126.30 | 127.67 | -81.2% |
| json-stream | warm | ms | 26.41 | 100.12 | 101.72 | +279.0% |
| json-stream | streaming | MB/s | 69.23 | 35.84 | 37.90 | -48.2% |
| compile-heavy | first-run | ms | 619.24 | 17.91 | 18.13 | -97.1% |
| compile-heavy | warm | ms | 1.07 | 0.86 | 0.84 | -19.7% |
| csv-to-json | first-run | ms | 595.66 | 16.63 | 17.06 | -97.2% |
| csv-to-json | warm | ms | 0.16 | 0.18 | 0.17 | +14.8% |
| xml-to-json | first-run | ms | 609.17 | 16.80 | 17.22 | -97.2% |
| xml-to-json | warm | ms | 0.17 | 0.19 | 0.17 | +13.6% |
| deep-selector | first-run | ms | 318.98 | 8.65 | 9.08 | -97.3% |
| deep-selector | warm | ms | 0.09 | 0.11 | 0.10 | +34.5% |
| group-by | first-run | ms | 809.04 | 199.03 | 201.40 | -75.4% |
| group-by | warm | ms | 70.83 | 168.45 | 171.07 | +137.8% |

## Charts

One chart per corpus case, one bar per runner (`engine`, `node-wrapper`, `python-wrapper`). A case's metrics differ in unit and scale, so each metric is a separate single-unit chart. `engine` is the table's delta baseline.

### trivial

```mermaid
xychart-beta
    title "trivial — cold-start (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [149.484, 38.142, 42.668]
```

```mermaid
xychart-beta
    title "trivial — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [309.206, 8.532, 8.775]
```

```mermaid
xychart-beta
    title "trivial — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.101, 0.09, 0.08]
```

### object-transform

```mermaid
xychart-beta
    title "object-transform — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [583.654, 16.731, 17.012]
```

```mermaid
xychart-beta
    title "object-transform — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.261, 0.236, 0.219]
```

### map-scale

```mermaid
xychart-beta
    title "map-scale — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [700.978, 145.616, 146.478]
```

```mermaid
xychart-beta
    title "map-scale — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [33.504, 117.512, 118.956]
```

```mermaid
xychart-beta
    title "map-scale — streaming (MB/s, higher is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "MB/s"
    bar [52.428, 28.532, 29.66]
```

### xml-to-csv

```mermaid
xychart-beta
    title "xml-to-csv — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [323.172, 8.615, 8.832]
```

```mermaid
xychart-beta
    title "xml-to-csv — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.104, 0.109, 0.094]
```

### json-stream

```mermaid
xychart-beta
    title "json-stream — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [673.213, 126.296, 127.674]
```

```mermaid
xychart-beta
    title "json-stream — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [26.415, 100.115, 101.718]
```

```mermaid
xychart-beta
    title "json-stream — streaming (MB/s, higher is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "MB/s"
    bar [69.226, 35.839, 37.904]
```

### compile-heavy

```mermaid
xychart-beta
    title "compile-heavy — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [619.235, 17.909, 18.126]
```

```mermaid
xychart-beta
    title "compile-heavy — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [1.072, 0.86, 0.843]
```

### csv-to-json

```mermaid
xychart-beta
    title "csv-to-json — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [595.66, 16.631, 17.056]
```

```mermaid
xychart-beta
    title "csv-to-json — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.161, 0.185, 0.166]
```

### xml-to-json

```mermaid
xychart-beta
    title "xml-to-json — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [609.166, 16.797, 17.218]
```

```mermaid
xychart-beta
    title "xml-to-json — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.168, 0.191, 0.173]
```

### deep-selector

```mermaid
xychart-beta
    title "deep-selector — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [318.976, 8.651, 9.078]
```

```mermaid
xychart-beta
    title "deep-selector — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [0.085, 0.115, 0.103]
```

### group-by

```mermaid
xychart-beta
    title "group-by — first-run (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [809.044, 199.029, 201.398]
```

```mermaid
xychart-beta
    title "group-by — warm (ms, lower is better)"
    x-axis ["engine", "node-wrapper", "python-wrapper"]
    y-axis "ms"
    bar [70.834, 168.445, 171.073]
```
