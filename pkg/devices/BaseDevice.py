# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import threading, Queue
import time
import logging
from pkg.utils.debug import debug_mesg

from pkg.utils.misc import is_valid_host


class Device(threading.Thread):
	def __init__(self, type, id, params):
		threading.Thread.__init__(self)
		self.type = type
		self.id = id
		self.params = params
		self.outputqueues = []
		self.description = "Device"
		self.statistics = [0,0,0,0] # [attempts, success, attempts_last, success_last]
		
		# take care of common parameters ... rest are device specific
		if 'sample_interval' in self.params:
			try:
				self.sample_interval = float(self.params['sample_interval'])
			except ValueError as e:
				logging.error("sample interval for device "+self.type+":"+self.id+" is not numeric.")
		if 'host' in self.params:
			try:
				x = self.params['host'].split(":")
				assert(len(x)<3)
				if (len(x)==2):
					self.host = x[0]
					self.port = int(x[1])
				else:
					self.host = x[0]
				assert(is_valid_host(self.host))
			except:
				logging.error("malformed host specification for device "+self.type+":"+self.id)
				exit(1)
		if 'port' in self.params:
			try:
				self.port = int(self.params['port'])
			except:
				logging.error("malformed port specification for device "+self.type+":"+self.id)
				exit(1)
		if 'timeout' in self.params:
			try:
				self.timeout = int(self.params['timeout'])
			except:
				logging.error("malformed timeout specification for device "+self.type+":"+self.id)
				exit(1)
		if 'url' in self.params:
			self.url = self.params['url']
		if 'serial' in self.params:
			self.serial = self.params['serial']
		self.sensor_names_map = self.params.get('sensor_names_map',{})			
			
	def attach_queue(self, q):
		self.outputqueues.append(q)
		
	def get_device_type(self):
		return self.type
		
	def get_device_id(self):
		return self.id
		
	def get_device_channels(self):
		pass
		
	def get_sample(self):
		pass
		
	def run(self):
		logging.debug("Running thread for device "+self.type+":"+self.id)
		while True:
			start_time = time.time()
			s = self.get_sample()
			if s:
				for q in self.outputqueues:
					q.put(s)
			diff=time.time()-start_time
			if hasattr(self, 'sample_interval'):
				time.sleep(max(self.sample_interval-diff,0))
			