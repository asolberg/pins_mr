#! /usr/bin/env python

from pymongo import Connection 
from bson.code import Code

conn = Connection('33.33.33.10', 27017)
db = conn.bookshelf
db = db.books

map = Code("function () {"
" book = this.book;"
" emit(book, {count : 1});"
" }")

reduce = Code("function (key, values) {"
    "var sum = 0;"
    "values.forEach(function(x) {"
    " sum += x.count;"
    "});"
    "var result = {"
    " count : sum "
    "};"
    "return result;"
    "}")

result = db.map_reduce(map, reduce, "test_out")
for doc in result.find():
  print doc
