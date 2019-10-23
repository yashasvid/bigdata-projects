#!/usr/bin/env python

import sys
import string

for line in sys.stdin:
    
	line = line.strip()
    
	row = line.split(",")
	print('{0}\t{1}'.format(row[2],1))


