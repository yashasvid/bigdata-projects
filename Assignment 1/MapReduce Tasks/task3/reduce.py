#!/usr/bin/env python
from operator import itemgetter
import sys

current_license_type = None
current_count = 0
current_sum = 0
license_type = None

# input comes from STDIN
for line in sys.stdin:
	
	line = line.strip()
	license_type, val = line.split('\t',1)
	count, amt_due = val.split(",")
	
	try:
		count = int(count)
		amt_due = float(amt_due)
	except ValueError:
		amt_due = 0.00
	#print(current_license_type,license_type)
	if current_license_type == license_type:
		current_count += count
		current_sum += amt_due
	else:
		if current_license_type:
			print('{0}\t{1:.2f}, {2:.2f}'.format(current_license_type, current_sum, current_sum/current_count))
		current_license_type = license_type
		current_count = count
		current_sum = amt_due

if current_license_type == license_type:
	print('{0}\t{1:.2f}, {2:.2f}'.format(current_license_type, current_sum, current_sum/current_count))
