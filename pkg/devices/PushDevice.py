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
import json
import cherrypy
import iso8601
import logging
from pkg.utils.debug import debug_mesg

from pkg.utils.misc import json_convert_unicode_to_string

class ApiVersion1(object):
	def __init__(self,grandparent):
		self.device = grandparent

	def upload(self, device_id):
		if (cherrypy.request.method!="POST"):
			return ""
		cl = cherrypy.request.headers['Content-Length']
		api_key_received = cherrypy.request.headers.get('X-ApiKey',None)
		rawbody = cherrypy.request.body.read(int(cl))
		body = json_convert_unicode_to_string(json.loads(rawbody))
		return self.device.handle_request(api_key_received,device_id,body)
		# return "Updated %r." % (body,)

	def index(self):
		return """
<html>
<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.4.2/jquery.min.js"></script>
<script type='text/javascript'>
function Update() {
    $.ajax({
      type: 'POST',
      url: "update",
      contentType: "application/json",
      processData: false,
      data: $('#updatebox').val(),
      success: function(data) {alert(data);},
      dataType: "text"
    });
}
</script>
<body>
<input type='textbox' id='updatebox' value='{}' size='20' />
<input type='submit' value='Update' onClick='Update(); return false' />
</body>
</html>
"""		
	upload.exposed=True
	index.exposed=True
	
	
class Api(object):
	pass

# this device starts a web server listening on a specified port waiting for device to push data
# data is in JSON format, modeled after Xively/COSM's format

class PushDevice(BaseDevice.Device):
	
	def __init__(self, id, params):
		super(PushDevice,self).__init__("PushDevice", id, params)
		if not hasattr(self,'port'):
			self.port=8200
		
		self.apikey = self.params.get('apikey',None)
		
		self.mode = self.params.get('mode',"cherrypy")
		
		self.drop_unknown_sensors = self.params.get('drop_unknown_sensors',True)
		if type(self.drop_unknown_sensors)==str:
			if self.drop_unknown_sensors.upper()=="TRUE":
				self.drop_unknown_sensors=True
			elif self.drop_unknown_sensors.upper()=="FALSE":
				self.drop_unknown_sensors=False
		if type(self.drop_unknown_sensors)!=bool:
			logging.error("parameter drop_unknown_sensors for device "+self.type+":"+self.id+" is invalid.")
			exit(1)
		
		self.sensors = self.params.get('sensors',{})
		if (type(self.sensors)!=list):
			self.sensors=[self.sensors]
		
		self.sensor_info={}
		for i,v in enumerate(self.sensors):
			if type(v)!=dict or (not 'datastream' in v) or (not 'unit' in v) or (not 'device' in v):
				logging.error("malformed sensor specification %s for device %s:%s"%(v,self.type,self.id))
				exit(1)
			if not v['device'] in self.sensor_info:
				self.sensor_info[v['device']]={}
			if not v['datastream'] in self.sensor_info[v['device']]:
				self.sensor_info[v['device']][v['datastream']]=(v.get('device_name',v['device']),v.get('datastream_name',v['datastream']),v['unit'])
		#print(self.sensor_info)
		debug_mesg("Created PushDevice with id: "+id)

		
	def get_sample(self):
		return None
		
	def get_device_channels(self):
		r = []
		for (f,fv) in self.sensor_info.iteritems():
			for (d,dv) in fv.iteritems():
				r.append((dv[0],dv[1],dv[2]))
		#print(r)
		return r
		
	def run(self):
		logging.debug("Running thread for device "+self.type+":"+self.id)
		if self.mode=="cherrypy":
			self.api=Api()
			self.api.v1=ApiVersion1(self)
			cherrypy.engine.autoreload.unsubscribe()
			cherrypy.quickstart(self)
		else:
			pass
		
	def validate_request(self,request):
		
		if not ((type(request)==dict) and ('version' in request) and ('datastreams' in request) and  (request['version']=="1.0.0") and (type(request['datastreams'])==list)):
			return False
		for ds in request['datastreams']:
			if not ((type(ds)==dict) and ('id' in ds) and ('datapoints' in ds) and (type(ds['datapoints'])==list)):
				return False
			for dp in ds['datapoints']:
				if not ((type(dp)==dict) and ('at' in dp) and ('value' in dp) and (type(dp['value'])==str)):
					return False
				try:
					iso8601.parse_date(dp['at'])
				except:
					return False
		return True

		
	def handle_request(self,api_key_received,device_id,body):
		if api_key_received==self.apikey:
			if not self.validate_request(body):
				raise cherrypy.HTTPError(status=400, message="Incorrect payload.")
			if not device_id in self.sensor_info:
				if self.drop_unknown_sensors:
					return "Unknown sensor dropped"
				s = {'device':device_id, 'datastreams':body['datastreams']}
				#print(s)
				for q in self.outputqueues:
					q.put(s)
			else:
				#print(body['datastreams'])
				for datastream in body['datastreams']:
					f = self.sensor_info[device_id]
					#print(datastream['id'])
					#print(f)
					s = {}
					if datastream['id'] in f:
						x = f[datastream['id']]
						s['device']=x[0]
						s['datastreams']=[{'id':x[1], 'datapoints':datastream['datapoints']}]
					else:
						if not self.drop_unknown_sensors:
							s = {'device':device_id, 'datastreams':[datastream]}
						else:
							return "Unknown sensor dropped"
					#print(s)
					for q in self.outputqueues:
						q.put(s)							
			return "Updated %r." % (body,)
		else:
			return "API Key Mismatch"
		
		
		
		