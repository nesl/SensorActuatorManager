#!/usr/bin/python

# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

from __future__ import print_function
from pprint import pprint
import argparse
import json
import sys, struct

import httplib
import xml.dom.minidom
import time
import threading, Queue
import os
import socket
import signal
import logging
from datetime import datetime

from pkg.utils.misc import json_convert_unicode_to_string, json_evaluate_expression

log_file = None

DEFAULT_DEBUG_LEVEL = 0

class SensorActuatorManager(object):
	def __init__(self, config_file_name):
		# read config file
		with open(config_file_name) as config_file: 
			try:
				self.config = json_evaluate_expression(json_convert_unicode_to_string(json.load(config_file)))
			except ValueError as e:
				print(e)
				logging.error("Malformed JSON in config file")
				exit(1)
			except:
				logging.error("Malformed JSON in config file")
				exit(1)
		logging.debug(self.config)
		
		# following is a dict with keys as device types and values as handles of loaded module code
		self.device_modules={}
		# following is a dict with keys as device ids and values as dicts with keys type, params, and handle of object
		self.devices={}
		# following is a dict with keys as service types and values as handles of loaded module code
		self.service_modules={}
		# following is a dict with keys as service ids and values as dicts with keys type, params, and handle of object
		self.services={}
		# following is a list of tuples (queue handle, id of device, id of service)
		self.queues = []
		
		if (not 'devices' in self.config):
			logging.error("Bad config file: no devices section")
			exit(1)

		if (not 'services' in self.config):
			logging.error("Bad config file: no services section")
			exit(1)
	
		if (type(self.config['devices'])!=list):
			self.config['devices'] = [self.config['devices']]
		if (type(self.config['services'])!=list):
			self.config['services'] = [self.config['services']]

		for s in self.config['services']:
			if (type(s)!=dict):
				logging.error("Malformed service specification - must be a dict")
				exit(1)
			if (('enable' in s) and (s['enable']!="True" and s['enable']!=True)):
				continue
			if (not 'id' in s):
				logging.error("anonymous service with no id")
				exit(1)
			if (s['id'] in self.services):
				logging.error("duplicate service id "+s['id'])
				exit(1)
			self.services[s['id']] = {}
			self.services[s['id']]['type'] = s['type']
			if ('params' in s):
				if (type(s['params'])!=dict):
					logging.error(" service "+s['id']+" has malformed parameters")
					exit(1)
				self.services[s['id']]['params'] = s['params']
			else:
				self.services[s['id']]['params'] = {}	
				
		# for each device in the config file
		for d in self.config['devices']:
			if (type(d)!=dict):
				logging.error("Malformed device specification - must be a dict")
				exit(1)
				
			if (('enable' in d) and (d['enable']!="True" and d['enable']!=True)):
				continue

			# check that the device has an id
			if (not 'id' in d):
				logging.error("anonymous device with no id")
				exit(1)

			# check that device id is unique
			if (d['id'] in self.devices):
				logging.error("duplicate device id "+d['id'])
				exit(1)
				
			# check that device has at least some services
			if (not 'services' in d):
				logging.error("device with no service")
				exit(1)

			# check that the device has a type
			if (not 'type' in d):
				logging.error(" device "+d['id']+" has no type specified")
				exit(1)

			# check that parameter list, if present, if properly formed
			if ('params' in d):
				if (type(d['params'])!=dict):
					logging.error(" device "+d['id']+" has malformed parameters")
					exit(1)
				
			# check that there is at least one service specified for this device
			if (type(d['services'])!=list):
				d['services'] = [d['services']]
			if (len(d['services'])<1):
				logging.error("must specify at least one service for device "+d['id'])
				exit(1)
				
			# import the device
			try:
				self.device_modules[d['type']] =  __import__('pkg.devices.'+d['type'], globals(), locals(), [d['type']], -1)
				logging.debug("Loaded code for device type "+d['type'])
			except ImportError as e:
				logging.exception("Unable to load device %s due to %s."%(d['type'],e))
				raise ImportError

			# now make an object of this device instance
			try:
				device_constructor = getattr(self.device_modules[d['type']], d['type'])
				logging.debug("Created object for device "+d['type']+":"+d['id'])
			except:
				logging.exception("ERROR: could not load class for device "+d['type'])
				exit(1)

			device_object = device_constructor(d['id'],d.get('params'))
			
			self.devices[d['id']] = {}
			self.devices[d['id']]['type'] = d['type']
			self.devices[d['id']]['services'] = d['services']	
			self.devices[d['id']]['params'] = d['params']		
			self.devices[d['id']]['handle'] = device_object
			
			for s in d['services']:
				if ((type(s)!=dict) or (not 'id' in s)):
					logging.error("Malformed service specification for device "+d['id'])
				if (('enable' in s) and (s['enable']!="True" and s['enable']!=True)):
					continue
				if (not s['id'] in self.services):
					logging.error("Service "+s['id']+" in device "+d['id']+" has no available configuration")
					exit(1)
				s['type'] = self.services[s['id']]['type']
				
				if (s['type'] not in self.service_modules):
					try:
						self.service_modules[s['type']] =  __import__('pkg.services.'+s['type'], globals(), locals(), [s['type']], -1)
						logging.debug("Loaded code for service type "+s['type'])
					except ImportError as e:
						logging.error("Unable to load service "+s['type'])
						exit(1)
				if (not 'handle' in self.services[s['id']]):
					try:
						service_constructor = getattr(self.service_modules[s['type']], s['type'])
						logging.debug("Created object for service "+s['type']+":"+s['id'])
					except:
						logging.error("could not load class for service "+d['type'])
						exit(1)
					service_object = service_constructor(s['id'],self.services[s['id']]['params'])
					self.services[s['id']]['handle'] = service_object
				else:
					service_object = self.services[s['id']]['handle']
				
				# create a queue connecting d to s
				q = Queue.Queue()
				self.queues.append((q,d['id'],s['id']))
				# store a pointer to the queue in the device object
				device_object.attach_queue(q)
				#print(d)
				# store a pointer to the queue in the service object together with parameters
				# specific to the device-service connection and also a pointer to the device
				service_object.attach_queue(q,s.get('params'),device_object)
			
		# clean up unused services
		services_to_delete=[]
		for s in self.services:
			if (not 'handle' in self.services[s]):	
				services_to_delete.append(s)
		for s in services_to_delete:
			logging.debug("Removing "+self.services[s]['type']+":"+s)
			del self.services[s]
		for d in self.devices:
			services_to_retain=[]
			for s in self.devices[d]['services']:
				if (('enable' in s) and (s['enable']!="True")):
					continue
				else:
					services_to_retain.append(s)
			self.devices[d]['services']=services_to_retain
	
	def start(self):
		# start all the service threads
		for s in self.services:
			self.services[s]['handle'].daemon=True
			self.services[s]['handle'].start()
		
		# start all the device threads
		for d in self.devices:
			self.devices[d]['handle'].daemon=True
			self.devices[d]['handle'].start()
					
if __name__ == "__main__":
		
	# read sensor configuration file
	parser = argparse.ArgumentParser()
	parser.add_argument("config", nargs='?', help="Configuration File, default: config.json", default="config.json")
	parser.add_argument("-l", "--loglevel", help="Logging Level, default: WARNING", default="WARNING")
	parser.add_argument("-i", "--include", help="List of devices to include, default: all", nargs='*')
	#parser.add_argument("-l", "--logfilename", help="Log file, default: stdout", type=argparse.FileType('a', 0), default="-")
	current_time = datetime.now().strftime("%Y%m%d")
	default_logfilename = "sam_logs/log_%s.txt"%(current_time)
	parser.add_argument("-f", "--logfilename", help="Log file, default: logs/log_YYYYmmdd", default=default_logfilename)
	
	args = parser.parse_args()
	
	numeric_level = getattr(logging, args.loglevel.upper(), None)
	if not isinstance(numeric_level, int):
		raise ValueError('Invalid log level: %s' % args.loglevel)

	d = os.path.dirname(args.logfilename)
	if not os.path.exists(os.path.dirname(args.logfilename)):
		os.makedirs(d)
				
	logging.basicConfig(filename=args.logfilename,level=numeric_level,format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
	logging.info("STARTING SAM")
	main = SensorActuatorManager(args.config)
	
	main.start()
	while True:
		pass
	
	