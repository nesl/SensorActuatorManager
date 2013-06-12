# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

from __future__ import print_function
from pprint import pprint

import logging

def set_debug_level(level):
	global debug_level
	debug_level=level

def debug_mesg(message):
	print(message)
	logging.debug(message)