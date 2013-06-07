# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import time
import Queue
import BaseDevice
import socket
import signal
#import httplib
import requests
import xml.dom.minidom
import logging

class TED5000(BaseDevice.Device):
	def __init__(self, id, params):
		super(TED5000,self).__init__("TED5000", id, params)
		self.i=0
		self.sensors = self.params.get('sensors',[])
		if not hasattr(self,'port'):
			self.port=80
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		print("Created TED5000 Device with id: "+id)

	def get_sample_test(self):
		self.i=self.i+1
		return (time.time(),self.i)

	def get_sample(self):
		# return self.get_sample_test()
		self.statistics[0] = self.statistics[0]+1
		reply = [None for _i in range(1+3*len(self.sensors))]
		volt=[]
		watt=[]
		voltampere=[]
		
		try:
			# conn = httplib.HTTPConnection(self.host,self.port,timeout=self.timeout)
			ted5000_url = "http://%s:%s/api/LiveData.xml"%(self.host,self.port)
			reply[0] = time.time()
			r = requests.get(ted5000_url,timeout=self.timeout)
			#conn.request("GET","/api/LiveData.xml")
			#res = conn.getresponse()
			#if (res.status == 200):
			if (r.status_code == requests.codes.ok):
				#xml_data = res.read()
				xml_data = r.text
				dom = xml.dom.minidom.parseString(xml_data)
				VoltageNow = dom.getElementsByTagName('VoltageNow')
				PowerNow = dom.getElementsByTagName('PowerNow')
				KVA = dom.getElementsByTagName('KVA')
				for i in range(1,len(self.sensors)+1):
					try:
						reply[i] = float(VoltageNow[i].childNodes[0].data)/10
					except:
						pass
					try:
						reply[len(self.sensors)+i] = float(PowerNow[i].childNodes[0].data)
					except:
						pass
					try:
						reply[2*len(self.sensors)+i] = float(KVA[i-1].childNodes[0].data)
					except:
						pass
					self.statistics[1] = self.statistics[1]+1
			else:
				loggin.error("Bad response %s from %s for %s.%s"%(res.status,self.host,self.type,self.id))
				reply = None
		except:
			reply = None
			loggin.error("Cannot communicate with %s for %s.%s"%(res.status,self.host,self.type,self.id))
		finally:
			#conn.close()
			# print(reply)
			return reply

	def get_device_channels(self):
		r = []
		r.extend([(self.sensor_names_map.get("Voltage[%s]"%(x),"Voltage[%s]"%(x)),"V") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("RealPower[%s]"%(x),"RealPower[%s]"%(x)),"W") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("ApparentPower[%s]"%(x),"ApparentPower[%s]"%(x)),"VA") for x in self.sensors])
		return r