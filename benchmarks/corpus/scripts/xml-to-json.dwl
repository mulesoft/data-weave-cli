output application/json
---
{
  books: payload.catalog.*book map (book) -> {
    title: book.title,
    price: book.price as Number
  }
}
