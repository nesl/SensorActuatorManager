# Author: Mani Srivastava, NESL, UCLA
# Created on: May 22, 2013
#
# Copyright notice in LICENSE file 
#

from __future__ import print_function
from pprint import pprint

debug_level = 0

def set_debug_level(level):
	global debug_level
	debug_level=level

def debug_mesg(message, level=1):
	global debug_level
	if (level<=debug_level):
		print(message)

def log_mesg(message):
	global log_file
	if log_file:
		pass
	else:
		print(message)