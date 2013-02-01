#! /usr/bin/env python
"""
A simple Map-Reduce algorithm to return the number of 
"keywords" and "boards" associated with a set of
"pin" documents from pinterest.
"""

__author__ = "Arian Solberg"
__version__ = "1.0.0"
__email__ = "asolberg@gmail.com"

from pymongo import Connection 
from bson.code import Code

conn = Connection('33.33.33.10', 27017)
db = conn.pin_mr
db = db.pins

# the "map" code. This arranges the raw data into
# the schema that we want and maps keywords and board
# names to a "count" of 1 that can later be used to sum
# up the results in Reduce.

map = Code("function () {"
  " domain = this.source_domain;"
  " var keywords_dict = {};"
  " var boards_dict = {};"
  " this._keywords.forEach(function(x) {"
  "   keywords_dict[x] = 1;"
  " });"
  " this.users.forEach(function(x) {"
  "   boards_dict[x.board_name] = 1;"
  " });"
  " emit(domain, {"
  " keywords : keywords_dict,"
  " board_names: boards_dict"
  " });"
  "}")

# "reduce" sums up the count variables
# and returns the same schema as before
reduce = Code("function (key, values) {"
  "var keywords_dict = {};"
  "var boards_dict = {};"
  "values.forEach(function(x) {"
  " for(var keyword in x.keywords) {"
  "   if ( !(keyword in keywords_dict) ) {"
  "     keywords_dict[keyword] = x.keywords[keyword];"
  "   }"
  "   else{"
  "     keywords_dict[keyword]+= x.keywords[keyword];"
  "   }"
  " }"
  " for(var board_name in x.board_names) {"
  "   if ( !(board_name in boards_dict) ) {"
  "     boards_dict[board_name] = x.board_names[board_name];"
  "   }"
  "   else{"
  "     boards_dict[board_name]+= x.board_names[board_name];"
  "   }"
  " }"
  "});"
  "var result = {"
  " keywords : keywords_dict,"
  " board_names: boards_dict"
  "};"
  "return result;"
  "}")

result = db.map_reduce(map, reduce, "mr_pin_count")
