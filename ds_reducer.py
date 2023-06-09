#!/usr/bin/python3
"""ds_reducer.py"""

import sys

current_word = None
current_count = 0
word = None


for line in sys.stdin:

	#line = line.strip()
	print(line)

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
