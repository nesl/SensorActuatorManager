# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseDevice
import httplib
import time
import json
import logging

from pkg.utils.misc import json_convert_unicode_to_string

class Vera(BaseDevice.Device):
	def __init__(self, id, params):
		super(Vera,self).__init__("Vera", id, params)
		if not hasattr(self,'port'):
			self.port=3480
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		self.sensors = self.params.get('sensors',{})
		if (type(self.sensors)!=list):
			self.sensors=[self.sensors]
		self.sensor_to_index_map={}
		for i,v in enumerate(self.sensors):
			if type(v)!=dict or (not 'id' in v) or (not 'unit' in v):
				logging.error("malformed sensor specification %s for device %s:%s"%(self.sensors,self.type,self.id))
				exit(1)
			try:
				v['id'] = int(v['id'])
			except:
				logging.error("non-integer id in sensor specification %s for device %s:%s"%(self.sensors,self.type,self.id))
				exit(1)
			k = (v.get('type',"devices"), v['id'])
			if not k in self.sensor_to_index_map:
				self.sensor_to_index_map[k]={v.get('field',"tripped"):(i+1)}
			else:
				self.sensor_to_index_map[k][v.get('field',"tripped")]=(i+1)
		print("Created Vera Device with id: "+id)
		
	def get_sample_test(self):
		self.i=self.i+1
		return (time.time(),self.i)
		
	def get_sample(self):
		# return self.get_sample_test()
		reply = [None for _i in range(1+len(self.sensors))]
		conn = httplib.HTTPConnection(self.host,self.port,timeout=self.timeout)
		try:
			conn.request("GET","/data_request?id=sdata&output_format=json")
			reply[0] = time.time()
			res = conn.getresponse()
			
			if (res.status == 200):
				try:
					json_data = json_convert_unicode_to_string(json.loads(res.read()))
					for d in json_data['devices']:
						for field,index in self.sensor_to_index_map.get(("devices",d['id']),{}).iteritems():
							if field in d:
								v = d[field]
								if (not (type(v)==int or type(v)==float)):
									v = int(v)
								reply[index] = v
					for d in json_data['scenes']:
						for field,index in self.sensor_to_index_map.get(("devices",d['id']),{}).iteritems():
							if field in d:
								v = d[field]
								if (not (type(v)==int or type(v)==float)):
									v = int(v)
								reply[index] = v						
					reply=tuple(reply)
				except:
					logging.error("problem in parsing response from %s:%s"%(self.type,self.id))
					reply=None
		except:
			logging.error("problem in getting data from %s:%s"%(self.type,self.id))
			reply=None
		#print(reply)
		return reply
		
	def get_device_channels(self):
		r = []
		for i in self.sensors:
			r.append((i.get('name',i['id']),i['unit']))
		return r