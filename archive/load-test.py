#! /usr/bin/env python

from pymongo import Connection
import json
import os
import random

conn = Connection('33.33.33.10', 27017)
db = conn.bookshelf
db = db.books

RECORDS = 101 
BOOKS = 1
TAGS = 5 

f = open('/usr/share/dict/words2', 'r')
words = f.read().splitlines()

d_len = len(words)
books = []
tags = []
boards = []
for i in range(BOOKS):
  books.append(words[random.randrange(0, d_len-1)]) 
for i in range(TAGS):
  tags.append(words[random.randrange(0, d_len-1)])
  if i % 3 == 0:
    tags[-1] = tags[-1] + ' ' + words[random.randrange(0, d_len-1)]

for i in range(RECORDS):
  record = {}
  r_kwords = 'good'
  record['tags'] = r_kwords
  record['book'] = random.choice(books)
  db.insert(record)
