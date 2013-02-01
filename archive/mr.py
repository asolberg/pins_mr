#! /usr/bin/env python

from pymongo import Connection 
from bson.code import Code

conn = Connection('33.33.33.10', 27017)
db = conn.pin_mr
db = db.pins

map = Code("function () {"
" domain = this.source_domain;"
" this._keywords.forEach(function(z) {"
" emit(domain, z);"
"});"
"}")

reduce = Code("function (key, values) {"
    "var d = {};"
    "for (var i = 0; i < values.length; i++) {"
    "   if ( !(values[i] in d) ) {"
    "     d[values[i]] = 1;"
    "   }"
    "   else{"
    "     d[values[i]]+=1;"
    "   }"
    "}"
    "print(d);"
    "return d;"
    "}")

result = db.map_reduce(map, reduce, "myresult")
for doc in result.find():
  print doc
