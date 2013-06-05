# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseDevice
import time
import httplib
import xml.dom.minidom
import logging

class eGaugeHTTP(BaseDevice.Device):
	def __init__(self, id, params):
		super(eGaugeHTTP,self).__init__("eGaugeHTTP", id, params)
		self.i=0
		if not hasattr(self,'port'):
			self.port=80
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		self.sensors = self.params.get('sensors',[])
		if (type(self.sensors)!=list):
			self.sensors=[self.sensors]
		self.sensor_to_index_map={}
		for i,v in enumerate(self.sensors):
			if type(v)!=dict or (not 'id' in v) or (not 'unit' in v):
				logging.error("malformed sensor specification %s for device %s:%s"%(self.sensors,self.type,self.id))
				exit(1)
			self.sensor_to_index_map[v['id']]=i+1
		#print(self.sensor_to_index_map)
		print("Created eGaugeHTTP Device with id: "+id)

	def get_sample_test(self):
		self.i=self.i+1
		r=[time.time()]
		r.extend([self.i for x in range(len(self.sensors))])
		return tuple(r)
				
	def get_sample(self):
		# return self.get_sample_test()
		reply = [None for _i in range(1+len(self.sensors))]
		conn = httplib.HTTPConnection(self.host,self.port,timeout=self.timeout)
		try:
			conn.request("GET","/cgi-bin/egauge?inst")
			reply[0] = time.time()
			res = conn.getresponse()
			if (res.status == 200):
				xml_data = res.read()
				dom = xml.dom.minidom.parseString(xml_data)
				if (dom.getElementsByTagName("data")):
					for r in dom.getElementsByTagName('r'):
						r_type = r.attributes["t"].value
						r_name = r.attributes["n"].value
						if r_name in self.sensor_to_index_map:
							reply[self.sensor_to_index_map[r_name]] = float(r.getElementsByTagName('i')[0].childNodes[0].data)	
			reply=tuple(reply)
		except:
			logging.error("problem in getting data from %s:%s"%(self.type,self.id))
			reply=None
		# print(xml_data)
		# print(reply)
		return reply
		
	def get_device_channels(self):
		r = []
		for i in self.sensors:
			r.append((i.get('name',i['id']),i['unit']))
		return r