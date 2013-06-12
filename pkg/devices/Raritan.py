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
import logging
from pkg.utils.debug import debug_mesg

from pysnmp.entity.rfc3413.oneliner import cmdgen

class Raritan(BaseDevice.Device):
	
	def __init__(self, id, params):
		super(Raritan,self).__init__("Raritan", id, params)
		self.i = 0
		if 'username' in params:
			self.username = params['username']
		else:
			self.username = ""
		if 'password' in params:
			self.password = params['password']
		else:
			self.password = ""
		if not hasattr(self,'port'):
			self.port=161
		if not hasattr(self,'host'):
			logging.error("no host name or IP address specified for device %s:%s"%(self.type,self.id))
			exit(1)
		self.sensors = self.params.get('sensors',[])
		if (type(self.sensors)!=list):
			self.sensors=[self.sensors]
		self.outlet_names_map = self.params.get('outlet_names_map',{})
		debug_mesg("Created Raritan Device with id: "+id)
	
	def get_sample_test(self):
		self.i=self.i+1
		r=[time.time()]
		r.extend([self.i for x in range(8)])
		r.extend([self.i for x in range(8)])
		r.extend([self.i for x in range(8)])
		r.extend([self.i for x in range(8)])
		r.extend([self.i for x in range(8)])
		return tuple(r)

	def get_sample(self):
		# return self.get_sample_test()
		count=0
		self.statistics[0] = self.statistics[0]+1
		current_time = time.time()
		errorIndication, errorStatus, errorIndex, varBinds = cmdgen.CommandGenerator().getCmd(
			cmdgen.CommunityData(self.username,self.password), # read-only
			# SNMP v3
			cmdgen.UdpTransportTarget((self.host, self.port)),
			# Current for 8 outlets (milliamps)
			(1,3,6,1,4,1,13742,4,1,2,2,1,4,1), (1,3,6,1,4,1,13742,4,1,2,2,1,4,2),
			(1,3,6,1,4,1,13742,4,1,2,2,1,4,3), (1,3,6,1,4,1,13742,4,1,2,2,1,4,4), 
			(1,3,6,1,4,1,13742,4,1,2,2,1,4,5), (1,3,6,1,4,1,13742,4,1,2,2,1,4,6),
			(1,3,6,1,4,1,13742,4,1,2,2,1,4,7), (1,3,6,1,4,1,13742,4,1,2,2,1,4,8),

			# Voltage for 8 outlets (millivolts)
			(1,3,6,1,4,1,13742,4,1,2,2,1,6,1), (1,3,6,1,4,1,13742,4,1,2,2,1,6,2),
			(1,3,6,1,4,1,13742,4,1,2,2,1,6,3), (1,3,6,1,4,1,13742,4,1,2,2,1,6,4),
			(1,3,6,1,4,1,13742,4,1,2,2,1,6,5), (1,3,6,1,4,1,13742,4,1,2,2,1,6,6),
			(1,3,6,1,4,1,13742,4,1,2,2,1,6,7), (1,3,6,1,4,1,13742,4,1,2,2,1,6,8),

        	# Active Power for 8 outlets (Watts)
			(1,3,6,1,4,1,13742,4,1,2,2,1,7,1), (1,3,6,1,4,1,13742,4,1,2,2,1,7,2),
			(1,3,6,1,4,1,13742,4,1,2,2,1,7,3), (1,3,6,1,4,1,13742,4,1,2,2,1,7,4),
			(1,3,6,1,4,1,13742,4,1,2,2,1,7,5), (1,3,6,1,4,1,13742,4,1,2,2,1,7,6),
			(1,3,6,1,4,1,13742,4,1,2,2,1,7,7), (1,3,6,1,4,1,13742,4,1,2,2,1,7,8),

			# Apparent Power for 8 outlets (Volt-amps)
			(1,3,6,1,4,1,13742,4,1,2,2,1,8,1), (1,3,6,1,4,1,13742,4,1,2,2,1,8,2),
			(1,3,6,1,4,1,13742,4,1,2,2,1,8,3), (1,3,6,1,4,1,13742,4,1,2,2,1,8,4),
			(1,3,6,1,4,1,13742,4,1,2,2,1,8,5), (1,3,6,1,4,1,13742,4,1,2,2,1,8,6),
			(1,3,6,1,4,1,13742,4,1,2,2,1,8,7), (1,3,6,1,4,1,13742,4,1,2,2,1,8,8),

			# Power Factor for 8 outlets (Percentage)
			(1,3,6,1,4,1,13742,4,1,2,2,1,9,1), (1,3,6,1,4,1,13742,4,1,2,2,1,9,2),
			(1,3,6,1,4,1,13742,4,1,2,2,1,9,3), (1,3,6,1,4,1,13742,4,1,2,2,1,9,4),
			(1,3,6,1,4,1,13742,4,1,2,2,1,9,5), (1,3,6,1,4,1,13742,4,1,2,2,1,9,6),
			(1,3,6,1,4,1,13742,4,1,2,2,1,9,7), (1,3,6,1,4,1,13742,4,1,2,2,1,9,8)
		)
		
		if errorIndication:
			logging.error("SNMP engine-level error %s for device %s:%s"%(errorIndication,self.type,self.id))
			return None
		else:
			if errorStatus:
				logging.error("SNMP PDU-level %s error %s for device %s:%s"%(
					errorStatus.prettyPrint(),varBinds[int(errorIndex)-1],self.type,self.id))
				return None
			else:
				reply = [None for _i in range(41)]
				reply[0] = current_time
				# following dict maps the SNMP channel to the start position in reply list
				map = {4:1,6:9,7:17,8:25,9:33}
				for n, v in varBinds:
					if (n[-2]<7):
						reply[map[n[-2]]+n[-1]-1] = int(v)/1000.0 # convert mA and mV to A and V
					else:
						reply[map[n[-2]]+n[-1]-1] = int(v)
				self.statistics[1] = self.statistics[1]+1
				return tuple(reply)
					
	def get_device_channels(self):
		r = []
		r.extend([(self.sensor_names_map.get("Current[%s]"%(x),"Current[%s]"%(x)),"A") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("Voltage[%s]"%(x),"Voltage[%s]"%(x)),"V") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("RealPower[%s]"%(x),"RealPower[%s]"%(x)),"W") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("ApparentPower[%s]"%(x),"ApparentPower[%s]"%(x)),"VA") for x in self.sensors])
		r.extend([(self.sensor_names_map.get("PowerFactor[%s]"%(x),"PowerFactor[%s]"%(x)),"%") for x in self.sensors])
		return r