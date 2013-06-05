# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import threading, Queue
import logging

class Service(threading.Thread):
	
	def __init__(self, type, id, params):
		threading.Thread.__init__(self)
		self.type = type
		self.id = id
		self.params = params
		#print("service %s:%s params = "%(self.type,self.id)),
		#print(params)
		self.inputqueues = []
		
	def attach_queue(self,q,p,h):
		device_type = h.get_device_type()
		device_id = h.get_device_id()
		device_channels = h.get_device_channels()
		device_channel_masks = None
		self.units_cache = {}
		
		for c in device_channels:
			if len(c)==2:
				self.units_cache[(q,c[0])]=c[1]
			elif len(c)==3:
				self.units_cache[(q,c[0],c[1])]=c[2]
		#print(self.units_cache)

		if (type(p)==dict and ('channel_mask' in p)):
			device_channel_masks = p['channel_mask']
			if (type(device_channel_masks)!=list):
				device_channel_masks = [device_channel_masks]
			if len(device_channel_masks)!=len(device_channels):
				logging.error("length of channel mask %s"%device_channel_masks+" differs from number of channels %s"%len(device_channels)+" for device "+device_type+":"+device_id+" for service "+self.type+":"+self.id)
				exit(1)
			for i, v in enumerate(device_channel_masks):
				if device_channel_masks[i]==True or device_channel_masks[i]=="True" or device_channel_masks[i]=="1" or device_channel_masks[i]==1:
					device_channel_masks[i]=True
				elif device_channel_masks[i]==False or device_channel_masks[i]=="False" or device_channel_masks[i]=="0" or device_channel_masks[i]==0:
					device_channel_masks[i]=False
				else:
					logging.error("incorrect channel mask value "+device_channel_masks[i]+" for device"+device_type+":"+device_id+" for service "+self.type+":"+self.id)
					exit(1)
		else:
			device_channel_masks = [True for x in range(len(device_channels))]
				
		if (type(p)==dict and ('channel_transform' in p)):
			device_channel_transforms = p['channel_transform']
			if (type(device_channel_transforms)!=list):
				device_channel_transforms = [device_channel_transforms]
			if len(device_channel_transforms)!=len(device_channels):
				logging.error("length of channel transform %s"%device_channel_transforms+" differs from number of channels %s"%len(device_channels)+" for device "+device_type+":"+device_id+" for service "+self.type+":"+self.id)
				exit(1)
			for i, v in enumerate(device_channel_transforms):
				try:
					device_channel_transforms[i][0]=float(device_channel_transforms[i][0])
					device_channel_transforms[i][1]=float(device_channel_transforms[i][1])
				except:
					logging.error("incorrect channel transform value %s for device %s for service %s"%(device_channel_transforms[i],device_id,self.id))
					exit(1)
		else:
			device_channel_transforms = [None for x in range(len(device_channels))]
		
		# store a tupl of the queue pointer, connection paramaters, and information about source device and its sensor channels
		self.inputqueues.append((q,p,(device_type,device_id,zip(device_channels,device_channel_masks,device_channel_transforms))))

	def process_sample(self,s,p,q):
		pass

	def run(self):
		logging.debug("Running thread for service "+self.type+":"+self.id)
		while True:
			for (q,p,d) in self.inputqueues:
				try:
					s = q.get(False)
				except Queue.Empty as e:
					s = None
				if s:
					self.process_sample(s,p,d,q)