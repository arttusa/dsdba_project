#!/usr/bin/python3
"""ds_mapper.py"""
import sys

# input comes from STDIN (standard input)
for i, line in enumerate(sys.stdin):
	if i == 0:
		continue
	# remove leading and trailing whitespace
	line = line.strip()
	values = line.split(",")
	zip_array = [values[4]] + values[0:4] + values[5:]
	print(",".join([str(elem) for elem in zip_array]))

    
    
# increase counters
# for word in words:
# write the results to STDOUT (standard output);
# what we output here will be the input for the
# Reduce step, i.e. the input for reducer.py
#
# tab-delimited; the trivial word count is 1
#print ( '%s\t%s' % (word, 1) )
        
# hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.0.jar -file /home/ds/dsdba_project/mapper.py -mapper mapper.py -file /home/ds/dsdba_project/reducer.py -reducer reducer.py -input /user/dsdba/data.csv -output /user/dsbda/test.txt

# hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.0.jar -file /home/ds/dsdba_project/ds_mapper.py -mapper ds_mapper.py -file /home/ds/dsdba_project/ds_reducer.py -reducer ds_reducer.py -input dsdba/data.csv -output dsbda/test

# hadoop fs -rm -r dsdba/test | hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.0.jar -file /home/ds/dsdba_project/ds_mapper.py -mapper ds_mapper.py -file /home/ds/dsdba_project/ds_reducer.py -reducer ds_reducer.py -input dsdba/data.csv -output dsdba/test

# hadoop fs -cat dsdba/test/part-00000

# hadoop fs -rm -r dsdba/test | hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.0.jar -file /home/ds/dsdba_project/ds_mapper.py -mapper ds_mapper.py -file /home/ds/dsdba_project/ds_reducer.py -reducer ds_reducer.py -input dsdba/data.csv -output dsdba/test | hadoop fs -cat dsdba/test/part-00000

# hadoop fs -rm -r dsdba/test | hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.10.0.jar -D mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D  mapred.text.key.comparator.options=-n -file /home/ds/dsdba_project/ds_mapper.py -mapper ds_mapper.py -file /home/ds/dsdba_project/ds_reducer.py -reducer ds_reducer.py -input dsdba/data.csv -output dsdba/test
