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

class EatonIQ260(ModbusDevice.TCPModbusDevice):

	def __init__(self, id, params):
		modbus_addr = 1
		modbus_func = 3
		# some more Eaton specific information
		self.reg_addr = 999
		self.sensors = [
			("Voltage[AN]", "V"), ("Voltage[BN]", "V"), ("Voltage[CN]", "V"),
			("Voltage[AB]", "V"), ("Voltage[BC]", "V"), ("Voltage[CA]", "V"),
			("Current[A]", "A"), ("Current[B]", "A"), ("Current[C]", "A"),
			("RealPower[Total]", "W"), ("ReactivePower[Total]", "VA"), ("ApparentPower[Total]"), ("PowerFactor[Total]", "%"), 
			("Frequency", "Hz"), ("Current[Neutral]", "A"),
			("RealPower[A]", "W"), ("RealPower[B]", "W"), ("RealPower[C]","W"), 
			("ReactivePower[A]", "VA"), ("ReactivePower[B]", "VA"), ("ReactivePower[C]", "VA"), 
			("ApparentPower[A]", "VA"), ("ApparentPower[B]", "VA"), ("ApparentPower[C]", "VA"), 
			("PowerFactor[A]", '%'), ("PowerFactor[B]", "%"), ("PowerFactor[C]", "%")
		]		
		
		super(EatonIQ260,self).__init__("EatonIQ260", id, modbus_addr, modbus_func, params)
		
		self.i = 0
		if not hasattr(self,'port'):
			self.port=4660
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		
		self.circuit_names_map = self.params.get('circuit_names_map',{})
		
		for (i,s) in enumerate(self.sensors):
			self.sensors[i] = (self.circuit_names_map.get(s[0],s[0]),s[1])

		print("Created EatonIQ260 Device with id: "+id)
		
	def get_sample_test(self):
		self.i=self.i+1
		reply=[time.time()]
		reply.extend([self.i for _i in range(len(self.sensors))])
		return tuple(reply)
		
	def get_sample(self):
		#return self.get_sample_test()
		self.statistics[0] = self.statistics[0]+1
		if not self.connect():
			return None
		current_time = time.time()
		reply = [time.time()]
		data = self.read_modbus_register(self.reg_addr,2*len(self.sensors),"f"*len(self.sensors))
		if data:
			for (i,v ) in enumerate(data):
				reply.append(v) 
		else:
			logging.error("error in getting data from device %s:%s"%(self.type,self.id))
			return(None)
		self.disconnect()
		self.statistics[1] = self.statistics[1]+1
		# fix units of PowerFactor to be %
		for i in [13, -3, -2, -1]:
			reply[i] = 100*reply[i]
		logging.debug(reply)
		return reply	
		
	def get_device_channels(self):
		return self.sensors