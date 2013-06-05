# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseDevice
import ModbusDevice
import time
import logging

class VerisE30A042(ModbusDevice.TCPModbusDevice):

	def __init__(self, id, params):
		modbus_addr = 1
		modbus_func = 3
		# some more Veris specific information
		self.max_meter_count = 42
		# specifications of meters
		# (base_register, count, name, unit)
		self.meter_types = [
			(2083, "RealPower", "kW"),
			(2167, "PowerFactor", "%"),
			(2251, "Current", "A")
		]
		
		super(VerisE30A042,self).__init__("VerisE30A042", id, modbus_addr, modbus_func, params)
		
		self.i = 0
		if not hasattr(self,'port'):
			self.port=4660
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		
		self.circuit_names_map = self.params.get('circuit_names_map',{})
		
		self.sensors = self.params.get('sensors',[str(i+1) for i in range(self.max_meter_count)])
		
		for (i,s) in enumerate(self.sensors):
			if s==False:
				self.sensors[i]=None
		if len(self.sensors)>self.max_meter_count:
			# truncate list of sensors
			self.sensors = self.sensors[0:self.max_meter_count]
		elif len(self.sensors)<self.max_meter_count:
			# no need to read all the meters
			self.max_meter_count = len(self.sensors)

		print("Created VerisE30 Device with id: "+id)
		
	def get_sample_test(self):
		self.i=self.i+1
		reply=[time.time()]
		for mt in self.meter_types:
			reply.extend([self.i for _i in range(self.max_meter_count)])
		return tuple(reply)
		
	def get_sample(self):
		#return self.get_sample_test()
		self.statistics[0] = self.statistics[0]+1
		if not self.connect():
			return None
		current_time = time.time()
		reply = [time.time()]
		for meter in self.meter_types:
			data = self.read_modbus_register(meter[0],2*self.max_meter_count,"f"*self.max_meter_count)
			if data:
				for (i,v) in enumerate(data):
					if self.sensors[i]:
						reply.append(v)
			else:
				logging.error("error in getting data from device %s:%s"%(self.type,self.id))
				return(None)
		self.disconnect()
		self.statistics[1] = self.statistics[1]+1
		debug_mesg(reply)
		return reply	
		
	def get_device_channels(self):
		r = []
		for reg in self.meter_types:
			reg_type = reg[1]
			reg_unit = reg[2]
			for s in self.sensors:
				if s:
					r.append((self.sensor_names_map.get("%s[%s]"%(reg_type,s),"%s[%s]"%(reg_type,s)),reg_unit))
		debug_mesg(r)
		return r