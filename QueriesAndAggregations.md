```agsl
GET sport/_search
{
  "query": {
    "combined_fields": {
      "query" : "футбол нога",
      "fields": ["title", "cotext"],
      "operator": "or"
    }
  }
}
```

```agsl

GET sport/_search
{
  "query": {
    "combined_fields": {
      "query" : "ronaldu Ronaldu",
      "fields": ["url"],
      "operator": "or"
    }
  }
}
```

```agsl
GET sport/_search 
{
   "query": {
     "match": {
       "product_name" : "Ronaldu"
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
