# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseService
import time
import json
import math
import random
import re
import threading, Queue
import requests 
from urlparse import urlparse
import logging
import calendar
import iso8601

from datetime import datetime

from pkg.utils.misc import match_and_replace, match_and_return, is_valid_host
from pkg.utils.debug import debug_mesg


class XivelyUploadHelper(threading.Thread):
	def __init__(self, parent):
		threading.Thread.__init__(self)
		self.parent = parent		

	def update_xively_feed(self,feed,datastreams,dpcnt):
		# we have to send values datastreams that have dpcnt datapoints
		# additionally we need to send anthing pending in self.parent.feed_state_info[6]
		
		# our basic strategy is to c
		# if dsflg is True, we will downsample so that only on API call is made
		
		xively_url = str.format(self.parent.api_url_format,feed)
		headers = {"X-ApiKey": self.parent.api_key}
		timestamp1 = time.time()
		
		pending_buffer = self.parent.feed_state_info[feed][6]
		
		pending_buffer_len=0
		for ds_id in pending_buffer:
			pending_buffer_len = pending_buffer_len + len(pending_buffer[ds_id])
			
		#print(pending_buffer)
		
		# put datastreams into pending_buffer
		for ds_id in datastreams:
			if ds_id in pending_buffer:
				pending_buffer[ds_id].extend(datastreams[ds_id])
			else:
				pending_buffer[ds_id] = datastreams[ds_id]
		
		total_dp_available = 0
		
		for ds_id in pending_buffer:
			total_dp_available = total_dp_available + len(pending_buffer[ds_id])
		
		
		num_of_dp_manageable = self.parent.upload_buffer_size + self.parent.max_datapoints_per_write
		
		drop_budget = max(0,total_dp_available-num_of_dp_manageable)
		drop_budget_left = drop_budget
		
		upload_budget = min(total_dp_available - drop_budget,  self.parent.max_datapoints_per_write)
		upload_budget_left = upload_budget
		
		body_toupload = {"version":"1.0.0", "datastreams":[]}
		body_todrop = {"version":"1.0.0", "feed":feed, "datastreams":[]}
		
		
		num_uploaded = 0
		num_dropped = 0
		
		for ds_id in pending_buffer:
			ds = pending_buffer[ds_id]
			ds_len = len(ds)
			
			ds_drop_count = int(min(math.ceil(drop_budget*ds_len/total_dp_available),drop_budget_left))
			num_dropped = num_dropped + ds_drop_count
			drop_budget_left = drop_budget_left - ds_drop_count
			dp = []
			#print(ds_id),
			#print(ds_drop_count),
			while ds_drop_count>0:
				# pop a random data point
				x = ds.pop(random.randint(0,len(ds)-1))
				dp.append(x)
				ds_drop_count = ds_drop_count - 1
			if dp != []:
				body_todrop["datastreams"].append({"id":ds_id, "datapoints":dp})
			
			# unfortunately the formula below leaves some underutilized upload capacity...				
			ds_upload_count = int(min(round(upload_budget*ds_len/total_dp_available),upload_budget_left))
			
			#print(ds_upload_count)
			num_uploaded = num_uploaded + ds_upload_count
			upload_budget_left = upload_budget_left - ds_upload_count
			if ds_upload_count>0:
				body_toupload["datastreams"].append({"id":ds_id, "datapoints":ds[0:ds_upload_count]})
				pending_buffer[ds_id] = ds[ds_upload_count:]			
		
		#print("uploading"),
		#print(headers),
		#print(body_toupload)
		logging.debug("%s Xively: feed=%s %s %s (%s+%s) %s %s | %s %s %s"%(datetime.now().isoformat(),feed,num_of_dp_manageable,
			total_dp_available,pending_buffer_len,dpcnt,upload_budget,drop_budget, num_dropped,num_uploaded,total_dp_available-num_uploaded-num_dropped))
		# upload all samples to be uploaded	
		try:
			r = None
			timestamp2 = time.time()
			r = requests.put(xively_url, data=json.dumps(body_toupload), headers=headers, timeout=self.parent.api_timeout, verify=False)
			timestamp3 = time.time()
			if r.status_code!=200:
				print(r.status_code),
				print(r.text)
				logging.error("HTTP Status Code: %s"%(r.status_code)),
				logging.error("HTTP Return Text: %s"%(r.text))
		except requests.exceptions.SSLError as e:
			timestamp3 = time.time()
			print("SSLError: %s"%(e))
			logging.error("SSLError: %s"%(e))
			if r!=None:
				print(r.status_code),
				print(r.text)
				logging.error("HTTP Status Code: %s"%(r.status_code))
				logging.error("HTTP Return Text: %s"%(r.text))
		except:
			timestamp3 = time.time()
			print("Unexpected Error: %s"%(sys.exc_info()[0]))
			logging.error("Unexpected Error: %s"%(sys.exc_info()[0]))
					
		# save all samples to be dropped into a file
		if body_todrop["datastreams"]!=[]:
			self.write_dropped_sample(feed,body_todrop,timestamp1)
		
		# return stats: (arrival_time,upload_start_time,upload_end_time,num_uploaded,num_dropped,num_buffered)
		
		return (timestamp1,timestamp2,timestamp3,num_uploaded,num_dropped,total_dp_available-num_uploaded-num_dropped)


	def write_dropped_sample(self,f,dropped_samples,timestamp):
		if self.parent.dropped_datapoint_save:
			dropped_datapoint_filename = "%s"%(datetime.fromtimestamp(time.time()).strftime("%Y%m%d"))
			if self.parent.feed_state_info[f][4]!=dropped_datapoint_filename or not self.parent.feed_state_info[f][5]:
				if self.parent.feed_state_info[f][5]:
					self.parent.feed_state_info[f][5].close()
					self.parent.feed_state_info[f][5]=None
				self.parent.feed_state_info[f][4]=dropped_datapoint_filename
				dropped_datapoint_folder = "%s/%s/%s"%(self.parent.dropped_datapoint_folder,f,datetime.fromtimestamp(timestamp).strftime("%Y"))
				try:
					if not os.path.exists(dropped_datapoint_folder):
						os.makedirs(dropped_datapoint_folder)
				except:
					logging.error("unable to create directory %s"%(dropped_datapoint_folder))
					return
				dropped_datapoint_full_filename = dropped_datapoint_folder+"/"+dropped_datapoint_filename
				try:
					self.parent.feed_state_info[f][5]=open(dropped_datapoint_full_filename,'a')
				except:
					logging.error("unable to open file %s"%(dropped_datapoint_full_filename))
					return
			self.parent.feed_state_info[f][5].write(json.dumps(dropped_samples))
			
	def run(self):
		while True:
			current_time = time.time()
			next_time_to_check = 2147483647 # maximum possible unix timestamp
			#print(self.parent.feed_state_info)
			for f in self.parent.feed_state_info:
				if current_time>=self.parent.feed_state_info[f][1]:
					# print("Uploading feed %s at %s"%(f,current_time))
					q = self.parent.feed_state_info[f][2]
					
					xively_message = {}
					
					num_datapoints = q.qsize()
					while not q.empty():
						item = q.get()
						# item is of the form (datastream_id, [datapoint_record, ...])
						if not item[0] in xively_message:
							xively_message[item[0]] = item[1]
						else:
							xively_message[item[0]].extend(item[1])						
					logging.debug(xively_message)
					
					# if this feed's queue was empty, there is no message to be sent... move on to the next one
					if xively_message=={}:
						continue
					
					
					logging.debug("# of datapoint [method 1] = %s"%num_datapoints),
						
					num_datapoints = 0
					for ds_id in xively_message:
						num_datapoints = num_datapoints + len(xively_message[ds_id])
						
					logging.debug("# of datapoint [method 2] = %s"%num_datapoints)
					
					update_result = self.update_xively_feed(f,xively_message,num_datapoints)
					
					#print(update_result)
					if update_result[5]>0:
						# some samples got buffered ... let us speed up a bit the polling of this feed
						debug_mesg("Speeding up Xively feed %s uploads!!!"%(f))
						time_for_next_upload_of_this_feed = current_time+min(self.parent.feed_state_info[f][0][1],self.parent.feed_state_info[f][0][0])
					else:
						time_for_next_upload_of_this_feed = current_time+self.parent.feed_state_info[f][0][0]
					self.parent.feed_state_info[f][1] = time_for_next_upload_of_this_feed
					next_time_to_check = min(next_time_to_check,time_for_next_upload_of_this_feed)
					
					self.parent.feed_state_info[f][3].append(update_result)
					
					# remove entries from too far back...
					while (update_result[0]-self.parent.feed_state_info[f][3][0][0])>self.parent.statistics_window:
						self.parent.feed_state_info[f][3].pop(0)
					
					# compute some stats
					upload_count = 0
					dropped_count = 0
					api_call_count = 0
					for e in self.parent.feed_state_info[f][3]:
						api_call_count = api_call_count + 1
						upload_count = upload_count + e[3]
						dropped_count = dropped_count + e[4]
					
					#print("Recent statistics for Xively feed %s:"%(f)),
					#print(self.parent.feed_state_info[f][3])
					#print("Next upload at %s"%time_for_next_upload_of_this_feed)
				else:
					next_time_to_check = min(next_time_to_check,self.parent.feed_state_info[f][1])
			if (next_time_to_check==2147483647):
				delta = 1
			else:
				delta=max(1,next_time_to_check-time.time())
			# print(next_time_to_check),
			# print(delta)
			time.sleep(delta)

class Xively(BaseService.Service):
	def __init__(self, id, params):
		super(Xively,self).__init__("Xively", id, params)
		
		try:
			self.api_url_format = params.get('api_url_format',"https://api.xively.com/v2/feeds/{0}.json")
			# do a test 
			test_url = str.format(self.api_url_format,1234)
			assert(test_url!=self.api_url_format) # just make sure that format had an effect			
			o = urlparse(test_url)
			self.api_protocol = o.scheme
			assert(self.api_protocol=="http" or self.api_protocol=="https")
			x = o.netloc.split(":")
			assert(len(x)<3)
			if (len(x)==2):
				self.api_host = x[0]
				self.api_port = int(x[1])
			else:
				self.api_host = x[0]
			assert(is_valid_host(self.api_host))	
		except:
			logging.error("parameter api_url_format = "+self.api_url_format+" for service "+self.type+":"+self.id+" is not valid.")
			exit(1)		
		
		try:
			self.api_timeout = float(params.get('api_timeout',"8"))
		except ValueError:
			logging.error("parameter api_timeout for service "+self.type+":"+self.id+" is not a float.")
			exit(1)
		
		if not 'api_key' in params:
			logging.error("no api_key for service %s:%s"%(self.type,self.id))
			exit(1)
		else:
			self.api_key = params.get('api_key',None)
		
		try:
			self.upload_interval = params.get('upload_interval',{})
			assert(type(self.upload_interval)==dict)
		except AssertionError as e:
			logging.error("parameter upload_interval for service "+self.type+":"+self.id+" is not a dict.")
			exit(1)

		try:
			self.upload_interval_burst = params.get('upload_interval_burst',{})
			assert(type(self.upload_interval_burst)==dict)
		except AssertionError as e:
			logging.error("parameter upload_interval_burst for service "+self.type+":"+self.id+" is not a dict.")
			exit(1)

		try:
			self.upload_buffer_size = int(params.get('upload_buffer_size',500))
		except ValueError:
			logging.error("parameter upload_buffer_size for service "+self.type+":"+self.id+" is not an integer.")
			exit(1)
						
		try:
			self.max_datapoints_per_write = int(params.get('max_datapoints_per_write',"490"))
		except ValueError as e:
			logging.error("parameter max_datapoints_per_write for service "+self.type+":"+self.id+" is not an integer.")
			exit(1)
		
		try:
			self.statistics_window = int(params.get('statistics_window',"120"))
			assert(self.statistics_window>=0)
		except ValueError as e:
			logging.error("parameter statistics_window for service "+self.type+":"+self.id+" is not a number.")
			exit(1)
		except AssertionError as e:
			logging.error("parameter statistics_window for service "+self.type+":"+self.id+" is negative.")
			exit(1)
				
		self.dropped_datapoint_save = params.get('dropped_datapoint_save',True)
		if type(self.dropped_datapoint_save)==str:
			if self.dropped_datapoint_save.upper()=="TRUE":
				self.dropped_datapoint_save=True
			elif self.dropped_datapoint_save.upper()=="FALSE":
				self.dropped_datapoint_save=False
		if type(self.dropped_datapoint_save)!=bool:
			logging.error("parameter dropped_datapoint_save for service "+self.type+":"+self.id+" is invalid.")
			exit(1)
						
		try:
			self.dropped_datapoint_folder = params.get('dropped_datapoint_folder',"./xively_dropped")
			assert(type(self.dropped_datapoint_folder)==str)
		except AssertionError as e:
			logging.error("parameter dropped_datapoint_folder = "+self.dropped_datapoint_folder+"for service "+self.type+":"+self.id+" is not a string.")
			exit(1)
			
		if self.dropped_datapoint_save and not os.path.exists(self.dropped_datapoint_folder):
			os.makedirs(self.dropped_datapoint_folder)
				
		# following are per-queue paramters
		self.xively_feeds = {}
		# self.xively_feed_maps = {}
		self.xively_datastream_maps={}
		
		# we store feed_id:[(upload_interval,upload_interval_burst), next_processing_time, queue,
		#                   [(arrival_time,upload_start_time,upload_end_time,num_uploaded,num_dropped,num_buffered)],
		#                   drop_filename, drop_filehandle, pending_buffer}
		self.feed_state_info={}
		
		#default update interval for feeds in seconds
		self.default_upload_interval = 15
		self.default_upload_interval_burst = 10
		
		self.uploader_thread = None
		
		debug_mesg("Created Xively Upload Service with id: "+id)
		
	def attach_queue(self,q,p,h):
		super(Xively,self).attach_queue(q,p,h)
		self.xively_feeds[q] = p.get('feed',None)
		
		# self.xively_feed_maps[q] = p.get('feed_map',{})
		self.xively_datastream_maps[q] = p.get('datastream_map',{})
		
		if not self.xively_feeds[q]:
			logging.error("no feed specified for samples from device %s:%s to service %s:%s. Dropping samples."%(d[0],d[1],self['type'],self['id']))
		else:
			feed_id = self.xively_feeds[q]
			if not feed_id in self.feed_state_info:
				try:
					feed_upload_interval = int(match_and_return(self.upload_interval,feed_id))
					assert(feed_upload_interval>0)
				except ValueError:
					logging.error("parameter upload_interval for feed "+feed_id+" in service "+self.type+":"+self.id+" is not an integer.")
					feed_upload_interval=self.default_upload_interval
				except AssertionError:
					logging.error("parameter  upload_interval for feed "+feed_id+" in service "+self.type+":"+self.id+" is not positive.")
					feed_upload_interval=self.default_upload_interval
				
				try:
					feed_upload_interval_burst = int(match_and_return(self.upload_interval_burst,feed_id))
					assert(feed_upload_interval_burst>0)
				except ValueError:
					logging.error("parameter upload_interval_burst for feed "+feed_id+" in service "+self.type+":"+self.id+" is not an integer.")
					feed_upload_interval_burst=self.default_upload_interval_burst
				except AssertionError:
					logging.error("parameter  feed_upload_interval_burst for feed "+feed_id+" in service "+self.type+":"+self.id+" is not positive.")
					feed_upload_interval_burst=self.default_upload_interval_burst
				# initialize feed_state_info entry for this feed
				self.feed_state_info[feed_id]=[(feed_upload_interval,feed_upload_interval_burst),0,Queue.Queue(),[],None,None,{}]
			if self.dropped_datapoint_save and not os.path.exists(self.dropped_datapoint_folder+"/"+feed_id):
				os.makedirs(self.dropped_datapoint_folder+"/"+feed_id)
				
		
	def process_sample(self,s,p,d,q):
		logging.debug(s)
		logging.debug(p)
		logging.debug(d)
		if not self.xively_feeds[q]:
			# no feed to deposit this sample to!
			logging.error("no feed specified for samples from device %s:%s to service %s:%s. Dropping samples."%(d[0],d[1],self['type'],self['id']))
		else:
			feed_id = self.xively_feeds[q]
			
			if type(s)==tuple or type(s)==list:
				dp_ts = s[0] # timestamp
				for i, (c,m,ct) in enumerate(d[2]):
					if not m:
						continue
					if s[i+1]==None:
						continue
					if 	ct!=None:
						try:
							dp_value = ct[0]*float(s[i+1])+ct[1]
						except:
							dp_value = s[i+1]
					else:
						dp_value = s[i+1]
					ds_id = match_and_replace(self.xively_datastream_maps[q],c[0])
					if type(ds_id)!=str:
						logging.error("datastream_maps %s yield invalid datastream id %s for input %s"%(self.xively_datastream_maps[q],ds_id,c[0]))
					else:
						item = (ds_id, [{"at":datetime.utcfromtimestamp(dp_ts).isoformat()+'Z', "value":"%s"%(dp_value)}])
						self.feed_state_info[feed_id][2].put(item)
			elif type(s)==dict:
				#print(s)
				devicenaame = s['device']
				for ds in s['datastreams']:
					ds_id_raw = "%s[%s]"%(devicenaame,ds['id'])
					#print(self.xively_datastream_maps[q]),
					#print(ds_id_raw)
					ds_id = match_and_replace(self.xively_datastream_maps[q],ds_id_raw)
					#print(type(ds_id))
					if type(ds_id)!=str:
						logging.error("datastream_maps %s yield invalid datastream id %s for input %s"%(self.xively_datastream_maps[q],ds_id_raw))
					else:
						item = (ds_id, ds['datapoints'])
						#print(item)
						self.feed_state_info[feed_id][2].put(item)
			else:
				logging.error("invalid type of sample %s received from queue by %s.%s"%(type(s),self.type,self.id))
	
		if not self.uploader_thread:
			#this is the first sample sent to this service
			#so let us start the uploader thread too
			self.uploader_thread = XivelyUploadHelper(self)
			self.uploader_thread.daemon=True
			self.uploader_thread.start()

		
		