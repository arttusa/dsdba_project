#!/usr/bin/python3
"""ds_reducer.py"""

import sys
from pymongo import MongoClient

conn = MongoClient()
db = conn.bank_loans

bank_loans_coll = db["bank_loans_hadoop"]

headers= ["ZIPCode", "ID", "Age", "Experience", "Income", "Family", "CCAvg", "Education", "Mortgage", "Personal Loan", "Securities Account", "CD Account", "Online", "CreditCard"]
# headers= ["ZIPCode", "Age", "Experience", "Income", "CCAvg", "Education", "Personal Loan", "Online", "CreditCard"]
# 1, 5, 8, 10, 11


# Check if
last_zip = None
zip_count = 0
current_zip_customers = 0


# input comes from STDIN (standard input)
for i, line in enumerate(sys.stdin):
	
	# split the line into words
	#words = line.split(",")
	row = {}
	line = line.strip()
	values = line.split(",")

	for index, value in enumerate(values):
		if index in [1, 5, 8, 10, 11]:
			continue

		header = headers[index]
		if header == "CCAvg":
			#row[] = int(value)
			row[header] = float(value)
		else:
			#row[header] = float(value)
			row[header] = int(value)
			
	
	current_zip = values[0] 
	
	if current_zip == last_zip:
		current_zip_customers = current_zip_customers + 1
	else:
		print ('Zip code: %s\tAmount of customers: %s' % (last_zip, current_zip_customers))
		current_zip_customers = 1
	
	bank_loans_coll.insert_one(row)
	print(row)
	last_zip = values[0]

print ('Zip code: %s\tAmount of customers: %s' % (last_zip, current_zip_customers))