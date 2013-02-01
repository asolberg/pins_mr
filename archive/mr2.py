#! /usr/bin/env python

from pymongo import Connection 
from bson.code import Code

conn = Connection('33.33.33.10', 27017)
db = conn.pin_mr
db = db.pins

map = Code("function () {"
" emit(this.source_domain, 1);"
"}")

reduce = Code("function (key, values) {"
    "var sum = 0;"
    "values.forEach(function(value) {"
    " sum += 1;"
    "});"
    "return sum;"
    "}")

result = db.map_reduce(map, reduce, "myresult")
for doc in result.find():
  print doc
