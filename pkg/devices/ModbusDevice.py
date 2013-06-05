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
import struct
import socket
import logging

from pkg.utils import crc16

class TCPModbusDevice(BaseDevice.Device):
	def __init__(self, type, id, mbaddr, mbfunc, params):
		super(TCPModbusDevice,self).__init__(type, id, params)
		self.modbus_addr = mbaddr
		self.modbus_func = mbfunc
		if not hasattr(self,'port'):
			self.port=4660
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		if not hasattr(self, 'timeout'):
			self.timeout = 2
		print("Created ModbusDevice with id: "+id)
		
	def get_device_channels(self):
		pass

	def get_sample(self):
		pass
		
	def connect(self, attempt_count=1, attempt_interval=5):
		while attempt_count>0:
			attempt_count = attempt_count-1				
			try:
				self.sock = socket.create_connection((self.host,self.port), self.timeout)
				return True
			except:
				print("Could not establish modbus connection to %s:%s."%(self.type,self.id))
				if attempt_count>0:
					print("Retrying in %s seconds..."%(attempt_interval))
					time.sleep(attempt_interval)
		return False
		
	def disconnect(self):
		try:
			self.sock.shutdown()
			self.sock.close()
		except:
			pass
	
	def read_modbus_register(self, reg_addr, reg_qty, parse_format, attempt_count=1, attempt_interval=5):
		
		# Create request with network endianness
		struct_format = ("!BBHH")
		packed_data = struct.pack(struct_format, self.modbus_addr, self.modbus_func, reg_addr, reg_qty)
		
		packed_data_size = struct.calcsize(struct_format)
		
		# Calculate the CRC16 and append to the end
		crc = crc16.calcCRC(packed_data)
		crc = socket.htons(crc)
		struct_format = ("!BBHHH")
		packed_data = struct.pack(struct_format, self.modbus_addr, self.modbus_func, reg_addr, reg_qty, crc)
		
		#print "Packed data: " + repr(packed_data)
	
		sent = False
		reply = None
		
		while sent==False and attempt_count>0:
			attempt_count = attempt_count-1
			try:
				# Send data
				self.sock.sendall(packed_data)
				reply = self.get_modbus_response(reg_qty,parse_format)
				sent = True
			except socket.error:
				if (attempt_count>0):
					logging.error("Connection closed by Modbus device %s:%s. Retrying in %s seconds..."%(self.type,self.id,attempt_interval))
					time.sleep(attempt_interval)
					self.connect()
				else:
					logging.error("Connection closed by Modbus device %s:%s."%(self.type,self.id))
		
		return reply
		
	def get_modbus_response(self, reg_qty, parse_format):
		# Response size is:
		#   Modbus Address 1 byte
		#   Function Code  1 byte
		#   Number of data bytes to follow 1 byte
		#   Register contents reg_qty * 2 b/c they are 16 bit values
		#   CRC 2 bytes
		response_size = 5 + 2*reg_qty
		response = self.sock.recv(response_size)
		
		struct_format = "!BBB" + parse_format + "H"
		
		try:
			data = struct.unpack(struct_format, response)
		except struct.error:
			logging.error("Received bad data from modbus on device %s:%s. Skipping..."%(self.type,self.id))
			return None
			
		# Remove first 3 bytes and last half-word (See above)
		return data[3:-1]
	