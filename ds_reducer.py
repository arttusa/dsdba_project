#!/usr/bin/python3
"""ds_reducer.py"""

import sys
from pymongo import MongoClient

conn = MongoClient()
db = conn.bank_loans

bank_loans_coll = db["bank_loans_hadoop_order_by_zip"]

headers= ["ZIPCode", "ID", "Age", "Experience", "Income", "Family", "CCAvg", "Education", "Mortgage", "Personal Loan", "Securities Account", "CD Account", "Online", "CreditCard"]

# input comes from STDIN (standard input)
for i, line in enumerate(sys.stdin):
    # split the line into words
    #words = line.split(",")
    row = {}
    line = line.strip()
    values = line.split(",")
    for index, value in enumerate(values):
    	header = headers[index]
    	row[header] = value
    print(row)
    bank_loans_coll.insert_one(row)

"""
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input you got from mapper.py
    word, count = line.split('\t', 1)
    count = int(count)

    # this IF-switch only works because Hadoop sorts map output by key
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write current result to STDOUT
            print ('%s\t%s' % (current_word, current_count))
        current_count = count
        current_word = word

# write the last result to STDOUT
if current_word:
    print ('%s\t%s' % (current_word, current_count))
    
"""
