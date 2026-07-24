output application/json deferred=true
---
payload map (item) -> { id: item.id, doubled: item.value * 2, label: "item_" ++ item.id }
