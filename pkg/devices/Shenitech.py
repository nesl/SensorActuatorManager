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
import logging
from pkg.utils.debug import debug_mesg

class Shenitech(BaseDevice.Device):
	def __init__(self, id, params):
		super(Shenitech,self).__init__("Shenitech", id, params)
		self.decription = "Shenitech_STUF200H"
		if not hasattr(self,'port'):
			self.port=80
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		debug_mesg("Created Shenitech Device with id: "+id)
		self.i=0
	
	def get_sample_test(self):
		self.i=self.i+1
		return (time.time(),self.i)
			
	def get_sample(self):
		# return self.get_sample_test()
		reply = None
		self.statistics[0] = self.statistics[0]+1
		try:
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect((self.host, self.port))
			s.settimeout(self.timeout)
			s.send('DQS\r\n')
			
			flow_reading = s.recv(32)
			current_time = time.time()
			s.close()
			if flow_reading[-1]=="\n":
				start = 0
				if not(flow_reading[0]=="+" or flow_reading[0]=="-"):
					start = flow_reading.find("+")
					if start==-1:
						start = flow_reading.find("-")
						if start!=-1 and flow_reading[start-1]=="E": start=-1
				if start!=-1: 
					end = flow_reading.find("m3")
					if end!=-1:
						reply = (current_time, 1000000*float(flow_reading[start:end]))
						self.statistics[1] = self.statistics[1]+1
		except:
			logging.error("Exception when reading device "+self.type+":"+self.id)
			reply = None
		finally:
			return reply
	
	def get_device_channels(self):
		return [(self.sensor_names_map.get("FlowRate","FlowRate"),"cm3/s")]