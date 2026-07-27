output application/json
---
{
  fullName: payload.first ++ " " ++ payload.last,
  adult: payload.age >= 18,
  initials: payload.first[0] ++ payload.last[0]
}
