# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import re
import os
import ConfigParser
import logging


# When reading in a JSON file into a dictionary, the elements are read in as unicode. 
# This function converts it to the strings
def json_convert_unicode_to_string(unicode_dict):
	if isinstance(unicode_dict, dict):
		return {json_convert_unicode_to_string(key): json_convert_unicode_to_string(value) for key, value in unicode_dict.iteritems()}
	elif isinstance(unicode_dict, list):
		return [json_convert_unicode_to_string(element) for element in unicode_dict]
	elif isinstance(unicode_dict, unicode):
		return unicode_dict.encode('utf-8')
	else:
		return unicode_dict



eval_dict = None


def json_evaluate_expression(json_data):
	global eval_dict
	if isinstance(json_data, dict):
		return {json_evaluate_expression(key): json_evaluate_expression(value) for key, value in json_data.iteritems()}
	elif isinstance(json_data, list):
		return [json_evaluate_expression(element) for element in json_data]
	elif isinstance(json_data, str) and (len(json_data)>2) and (json_data[0:2]=="%%"):
		if eval_dict==None:
			eval_dict = dict(os.environ)
			# read sam.ini file
			json_params = []
			for f in ["sam_conf", ".sam_conf", "~/sam_conf", "~/.sam_conf", "~/lib/sam_conf", "~/lib/.sam_conf", "~/local/lib/sam_conf", "~/local/lib/.sam_conf" ]:
				try:
					c = ConfigParser.SafeConfigParser()
					c.optionxform = str
					c.read(os.path.expanduser(f))
					json_params = c.items('JSON')
					break
				except:
					pass
			for i in json_params:
				eval_dict[i[0]] = i[1]
			# eval_dict['__builtins__']=locals()['__builtins__']
			
		try:
			return eval(json_data[2:],{},eval_dict)
		except:
			logging.exception("problem when evaluation expressions in JSON.")
			print("ERROR: Problem when evaluation expressions in JSON. Check log file.")
			raise
	else:
		return json_data

def match_and_replace(mapdict, input_string):
	default_rule=None
	for k in mapdict:
		if k==r".*":
			default_rule=mapdict[k]
			continue
		m=re.match(k,input_string)
		if m:
			return m.expand(mapdict[k])
	if default_rule!=None:
		m=re.match(".*",input_string)
		return m.expand(default_rule)
	return None
	
def match_and_return(mapdict, input_string):
	default_reply=None
	for k in mapdict:
		if k==r".*":
			default_reply=mapdict[k]
			continue
		m=re.match(k,input_string)
		if m:
			return mapdict[k]
	return default_reply
	
def is_valid_host(host):
	ValidIpAddressRegex = r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";
	ValidHostnameRegex = r"^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$";
	if re.match(ValidIpAddressRegex,host):
		return True
	elif len(host)<=255 and re.match(ValidHostnameRegex,host):
		return True
	else:
		return False
