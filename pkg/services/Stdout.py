# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseService
import calendar
import iso8601

class Stdout(BaseService.Service):
	def __init__(self, id, params):
		super(Stdout,self).__init__("Stdout", id, params)
		print("Created Stdout Service with id: "+id)
	
	def process_sample(self,s,p,d,q):
		debug_mesg(s)
		debug_mesg(p)
		debug_mesg(d)
		if type(s)==tuple or type(s)==list:
			o = "%s,%s,%s"%(d[0],d[1],s[0])
			for i, (c,m,ct) in enumerate(d[2]):
				if not m:
					continue
				if s[i+1]==None:
					continue
				if ct!=None:
					try:
						x = ct[0]*float(s[i+1])+ct[1]
					except:
						x = s[i+1]
				else:
					x = s[i+1]
				o = o+",%s,%s,%s"%(c[0],x,c[1])
			print(o)
		elif type(s)==dict:
			feed = s['feed']
			for ds in s['datastreams']:
				for dp in ds['datapoints']:
					o="%s,%s,%s,%s[%s],%s,%s"%(d[0],d[1],calendar.timegm(iso8601.parse_date(dp['at']).utctimetuple()),feed,ds['id'],dp['value'],self.units_cache.get((q,feed,ds['id']),"unknown"))
					print(o)

			
		