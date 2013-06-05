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
import time
import logging

class File(BaseService.Service):
	def __init__(self, id, params):
		super(File,self).__init__("File", id, params)
		self.folder = params.get('folder',"sam_out")
		self.prefix = params.get('prefix',"%s"%(self.id))
		if not 'flush_interval' in params:
			self.flush_interval = 5
		else:
			try:
				self.flush_interval = int(params['flush_interval'])
			except:
				logging.error("malformed flush_interval for service %s:%s"%(self.type,self.id))
				exit(1)
		self.open_file_name=None
		self.open_file_handle=None
		print("Created File Output Service with id: "+id)

	def process_sample(self,s,p,d,q):
		logging.debug(s)
		logging.debug(p)
		logging.debug(d)
		if type(s)==tuple or type(s)==list:
			ts = s[0]
			o = "%s,%s,%s"%(d[0],d[1],ts)
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
			self.write_sample(ts,o+"\n")
		elif type(s)==dict:
			feed = s['feed']
			for ds in s['datastreams']:
				for dp in ds['datapoints']:
					ts=calendar.timegm(iso8601.parse_date(dp['at']).utctimetuple())
					o="%s,%s,%s,%s[%s],%s,%s\n"%(d[0],d[1],ts,feed,ds['id'],dp['value'],self.units_cache.get((q,feed,ds['id']),"unknown"))
					self.write_sample(ts,o)
					
	def write_sample(self,ts,s):
		sample_time=time.localtime(float(ts)) 
		file_directory="%s/%s"%(self.folder,sample_time[0])
		file_fullname="%s/%s%s.txt"%(file_directory,self.prefix,"%s_%02d_%02d"%(sample_time[0],sample_time[1],sample_time[2]))
		try:
			if not os.path.exists(file_directory):
				os.makedirs(file_directory)
		except:
			logging.error("unable to create directory %s"%(file_directory))
			return
			
		if (self.open_file_handle!=None and self.open_file_name!=file_fullname):
			try:
				self.open_file_handle.close()
			except:
				pass
			self.open_file_handle=None
		if not self.open_file_handle:
			self.open_file_name=file_fullname
			try:
				self.open_file_handle=open(self.open_file_name,'a')
			except:
				logging.error("unable to open file %s"%(self.open_file_name))
		if self.open_file_handle:
			self.open_file_handle.write(s)
			
			
		
		