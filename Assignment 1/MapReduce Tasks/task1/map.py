#!/usr/bin/env python
import sys
import os
from csv import reader
import string

for line in sys.stdin:
	#line = line.strip()
	#if '"' in line:
	#	values_3 = line.split('"')
	#	line = values_3[0] + values_3[1].replace(",", "~")+values_3[2]

	line = reader([line],delimiter =',')
	input_file = os.environ.get('mapreduce_map_input_file')
	column = list(line)[0]

	if len(column) ==22:
		print('{0}\t{1}, {2}, {3}, {4}'.format(column[0],column[14],column[6],column[2],column[1]))
	else:
		print('{0}\t{1}'.format(column[0],"openparking"))	
