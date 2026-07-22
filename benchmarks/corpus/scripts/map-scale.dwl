output application/json
---
payload map (item) -> { id: item.id, doubled: item.value * 2, label: "item_" ++ item.id }
