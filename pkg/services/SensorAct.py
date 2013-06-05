# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

import sys
import os
import Queue
import BaseService
import logging

class SensorAct(BaseService.Service):
	def __init__(self, id, params):
		super(SensorAct,self).__init__("SensorAct", id)
		print("Created SensorAct:"+id)