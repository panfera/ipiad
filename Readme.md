![Агрегация по авторам](./images/authors.png)
![Агрегация по авторам и датам](./images/authors_dates.png)
```agsl
GET news/_search
{
  "query": {
    "combined_fields": {
      "query" : "Путин",
      "fields": ["title", "cotext"]
    }
  }
}
```

```agsl

GET news/_search
{
  "query": {
    "combined_fields": {
      "query" : "Путин Кадыров",
      "fields": ["title", "cotext"],
      "operator": "or"
    }
  }
}
```

```agsl
GET news/_search
{
  "query": {
    "combined_fields": {
      "query" : "Пригожин Кадыров",
      "fields": ["title", "cotext"],
      "operator": "and"
    }
  }
}
```
```
 GET sport/_search
{
  "query": {
    "multi_match" : {
        "query": "клуб",
        "fields": ["title", "context"],
        "type": "cross_fields"
    }
  }
}
```
