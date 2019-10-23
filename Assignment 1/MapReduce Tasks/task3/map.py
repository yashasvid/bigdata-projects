#!/usr/bin/env python

import sys
import string

for line in sys.stdin:
    
	line = line.strip()

	if '"' in line:

		values_3 = line.split('"')
		line = values_3[0] + values_3[1].replace(",", "$")+values_3[2]

	row = line.split(",")
	print('{0}\t{1},{2}'.format(row[2],1,row[12]))


