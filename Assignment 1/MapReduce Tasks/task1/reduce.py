#!/usr/bin/env python
import sys
import string

#curr_summ = None
#curr_val = None
#curr_count = 0
#summ = None
prevKey = None
prevVal = None
keycount = 0

for line in sys.stdin:
	line = line.strip()
	#summ, val = line.split("\t",1)
	key,value = line.split('\t') 
#  	if(curr_summ ==summ):
#		curr_count+=1
#	else:
#		if curr_summ:
#			if curr_count==1 and curr_val!="open":
#				print('{0}\t{1}'.format(curr_summ,curr_val.replace("~",",")))
#		curr_summ = summ
#		curr_count = 1
#		curr_val = val
#if curr_count==1 and curr_val!="open":
#	print('{0}\t{1}'.format(curr_summ,curr_val.replace("~",",")))
	currentKey =key
	if(currentKey!=prevKey and prevKey!=None):
		if keycount==1:
			if prevVal!='openparking':
				print('{0}\t{1}'.format(prevKey,prevVal))
		prevKey = currentKey
		prevVal = value
		keycount = 1
	else:
		prevKey = currentKey
		prevVal =value
		keycount +=1

if currentKey !=prevKey and keycount==1 and prevVal!='open':
	print('{0}\t{1}'.format(prevKey,prevVal))







	

