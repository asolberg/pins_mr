#! /usr/bin/env python

from pymongo import Connection 
from bson.code import Code

conn = Connection('33.33.33.10', 27017)
db = conn.bookshelf
db = db.books

map = Code("function () {"
" book = this.book;"
" tags_dict = {};"
" this.tags.forEach(function(x) {"
"   tags_dict[x] = 1;"
" });"
" emit(book, {"
" tags : tags_dict"
" });"
"}")

reduce = Code("function (key, values) {"
    "var tags_dict = {};"
    "values.forEach(function(x) {"
    " for(var keyword in x.tags) {"
    "   if ( !(keyword in tags_dict) ) {"
    "     tags_dict[keyword] = 1;"
    "   } else {"
    "     tags_dict[keyword] += 1;"
    "   }"
    " }"
    "});"
    "var result = {"
    " tags : tags_dict "
    "};"
    "return result;"
    "}")

result = db.map_reduce(map, reduce, "test_out")
for doc in result.find():
  print doc
