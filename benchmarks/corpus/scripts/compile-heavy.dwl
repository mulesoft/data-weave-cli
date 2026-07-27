output application/json
---
{
  a: (1 to 100) reduce ((i, acc = 0) -> acc + i),
  b: [1, 2, 3, 4, 5] map ($ * 2) filter ($ > 4) reduce ((i, acc = 0) -> acc + i),
  c: { x: 1, y: 2, z: 3 } mapObject (v, k) -> { (k): v * 10 },
  d: "hello world" splitBy " " map upper($),
  e: (1 to 50) map (n) -> { n: n, sq: n * n, even: (n mod 2) == 0 }
}
