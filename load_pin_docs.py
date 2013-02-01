#! /usr/bin/env python
"""
A simple "pin" document generator
to load records into Mongo that resemble
documents from pinterest.
"""

__author__ = "Arian Solberg"
__version__ = "1.0.0"
__email__ = "asolberg@gmail.com"

from pymongo import Connection
import random

conn = Connection('33.33.33.10', 27017)
db = conn.pin_mr
db = db.pins

# program parameters
# number of records/documents to insert into mongo
RECORDS = 200000

# these three set the potential sample size for each data structure
DOMAINS = 400
KEYWORDS = 400 
BOARDS = 400

#load a list of english words
f = open('/usr/share/dict/words2', 'r')
words = f.read().splitlines()
f.close()

#make three lists that we will draw from to populate our documents
domains = random.sample(words, DOMAINS) 
keywords = random.sample(words, KEYWORDS)
# make every 5th keyword a double keyword separated by a space, just for fun
keywords = [j if i%5!=0 and i!=0 else j + ' ' + random.choice(keywords) for i,j in enumerate(keywords)]
boards = random.sample(words, BOARDS)

for i in range(RECORDS):
  record = {}
  
  # get up to 5 random keywords to apply to this pin
  r_kwords = random.sample(keywords, random.randrange(1,5))
  r_boards = random.sample(boards, random.randrange(1,5))
  users = []
  #each "board" dict is inserted into a 'users' array.
  #we're only populating with the data that we're interested in
  for b in r_boards:
    user_dict = {}
    user_dict['board_name'] = b
    users.append(user_dict)
  record['_keywords'] = r_kwords
  record['users'] = users
  record['source_domain'] = random.choice(domains) + '.com'
  db.insert(record)
