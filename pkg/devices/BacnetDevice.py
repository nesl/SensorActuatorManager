# Author: Mani Srivastava, NESL, UCLA
# Created on: May 31, 2013
#
# Copyright notice in LICENSE file 
#
# This file includes code adapted from sample application at http://bacpypes.sourceforge.net
# and for such code the original copyright terms apply
#

# read 10.0.0.228 analogInput 196608 presentValue

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
from pkg.utils.debug import debug_mesg

from pkg.utils import crc16

import logging

from ConfigParser import ConfigParser

from bacpypes.debugging import Logging, ModuleLogger
from bacpypes.consolelogging import ConsoleLogHandler
from bacpypes.consolecmd import ConsoleCmd

import bacpypes.core

from bacpypes.pdu import Address, GlobalBroadcast
from bacpypes.app import LocalDeviceObject, BIPSimpleApplication
from bacpypes.object import get_object_class, get_datatype

from bacpypes.apdu import WhoIsRequest, IAmRequest, ReadPropertyRequest, Error, AbortPDU, ReadPropertyACK
from bacpypes.primitivedata import Unsigned
from bacpypes.constructeddata import Array
from bacpypes.basetypes import ServicesSupported, BACnetPropertyReference
from bacpypes.errors import DecodingError

# some debugging
_debug = 0
_log = ModuleLogger(globals())

# reference a simple application
thisApplication = None

#
#   BacnetApplication
#

class BacnetApplication(BIPSimpleApplication, Logging):

    def __init__(self, *args):
        if _debug: BacnetApplication._debug("__init__ %r", args)
        BIPSimpleApplication.__init__(self, *args)

        # keep track of requests to line up responses
        self._request = None

    def request(self, apdu):
        if _debug: BacnetApplication._debug("request %r", apdu)

        # save a copy of the request
        self._request = apdu

        # forward it along
        BIPSimpleApplication.request(self, apdu)

    def confirmation(self, apdu):
        if _debug: BacnetApplication._debug("confirmation %r", apdu)

        if isinstance(apdu, Error):
            sys.stdout.write("error: %s\n" % (apdu.errorCode,))
            sys.stdout.flush()

        elif isinstance(apdu, AbortPDU):
            apdu.debug_contents()

        elif (isinstance(self._request, ReadPropertyRequest)) and (isinstance(apdu, ReadPropertyACK)):
            # find the datatype
            datatype = get_datatype(apdu.objectIdentifier[0], apdu.propertyIdentifier)
            BacnetApplication._debug("    - datatype: %r", datatype)
            if not datatype:
                raise TypeError, "unknown datatype"

            # special case for array parts, others are managed by cast_out
            if issubclass(datatype, Array) and (apdu.propertyArrayIndex is not None):
                if apdu.propertyArrayIndex == 0:
                    value = apdu.propertyValue.cast_out(Unsigned)
                else:
                    value = apdu.propertyValue.cast_out(datatype.subtype)
            else:
                value = apdu.propertyValue.cast_out(datatype)
            BacnetApplication._debug("    - value: %r", value)

            sys.stdout.write(str(value) + '\n')
            sys.stdout.flush()

    def indication(self, apdu):
        if _debug: BacnetApplication._debug("indication %r", apdu)

        if (isinstance(self._request, WhoIsRequest)) and (isinstance(apdu, IAmRequest)):
            device_type, device_instance = apdu.iAmDeviceIdentifier
            if device_type != 'device':
                raise DecodingError, "invalid object type"

            if (self._request.deviceInstanceRangeLowLimit is not None) and \
                (device_instance < self._request.deviceInstanceRangeLowLimit):
                pass
            elif (self._request.deviceInstanceRangeHighLimit is not None) and \
                (device_instance > self._request.deviceInstanceRangeHighLimit):
                pass
            else:
                # print out the contents
                sys.stdout.write('pduSource = ' + repr(apdu.pduSource) + '\n')
                sys.stdout.write('iAmDeviceIdentifier = ' + str(apdu.iAmDeviceIdentifier) + '\n')
                sys.stdout.write('maxAPDULengthAccepted = ' + str(apdu.maxAPDULengthAccepted) + '\n')
                sys.stdout.write('segmentationSupported = ' + str(apdu.segmentationSupported) + '\n')
                sys.stdout.write('vendorID = ' + str(apdu.vendorID) + '\n')
                sys.stdout.flush()

        # forward it along
        BIPSimpleApplication.indication(self, apdu)

#
#   isint
#

def isint(s):
    """Return true if s is all digits."""
    for c in s:
        if c not in '0123456789':
            return False
    return True

#
#   BacnetConsoleCmd
#

class BacnetConsoleCmd(ConsoleCmd, Logging):
    def run(self):
        if _debug: ConsoleCmd._debug("run")

        # run the command loop
        #self.cmdloop()
        #self.do_read("10.0.0.228 analogInput 196608 presentValue")
        time.sleep(0.1)
        while True:
	        self.do_read("10.0.0.228 analogInput 196608 presentValue")
	        self.do_read("10.0.0.228 analogInput 196608 presentValue")
	        time.sleep(5)
	
        if _debug: ConsoleCmd._debug("    - done cmdloop")

        # tell the main thread for this device to stop, this thread will exit
        bacpypes.core.stop()
	
    def do_whois(self, args):
        """whois [ <addr>] [ <lolimit> <hilimit> ]"""
        args = args.split()
        if _debug: BacnetConsoleCmd._debug("do_whois %r", args)

        try:
            # build a request
            request = WhoIsRequest()
            if (len(args) == 1) or (len(args) == 3):
                request.pduDestination = Address(args[0])
                del args[0]
            else:
                request.pduDestination = GlobalBroadcast()

            if len(args) == 2:
                request.deviceInstanceRangeLowLimit = int(args[0])
                request.deviceInstanceRangeHighLimit = int(args[1])
            if _debug: BacnetConsoleCmd._debug("    - request: %r", request)

            # give it to the application
            thisApplication.request(request)

        except Exception, e:
            BacnetConsoleCmd._exception("exception: %r", e)

    def do_iam(self, args):
        """iam"""
        args = args.split()
        if _debug: BacnetConsoleCmd._debug("do_iam %r", args)

        try:
            # build a request
            request = IAmRequest()
            request.pduDestination = GlobalBroadcast()

            # set the parameters from the device object
            request.iAmDeviceIdentifier = thisDevice.objectIdentifier
            request.maxAPDULengthAccepted = thisDevice.maxApduLengthAccepted
            request.segmentationSupported = thisDevice.segmentationSupported
            request.vendorID = thisDevice.vendorIdentifier
            if _debug: BacnetConsoleCmd._debug("    - request: %r", request)

            # give it to the application
            thisApplication.request(request)

        except Exception, e:
            BacnetConsoleCmd._exception("exception: %r", e)

    def do_read(self, args):
        """read <addr> <type> <inst> <prop> [ <indx> ]"""
        
        args = args.split()
        if _debug: BacnetConsoleCmd._debug("do_read %r", args)

        try:
            addr, obj_type, obj_inst, prop_id = args[:4]

            if isint(obj_type):
                obj_type = int(obj_type)
            elif not get_object_class(obj_type):
                raise ValueError, "unknown object type"

            obj_inst = int(obj_inst)

            datatype = get_datatype(obj_type, prop_id)
            if not datatype:
                raise ValueError, "invalid property for object type"

            # build a request
            request = ReadPropertyRequest(
                objectIdentifier=(obj_type, obj_inst),
                propertyIdentifier=prop_id,
                )
            request.pduDestination = Address(addr)

            if len(args) == 5:
                request.propertyArrayIndex = int(args[4])
            if _debug: BacnetConsoleCmd._debug("    - request: %r", request)

            # give it to the application
            thisApplication.request(request)

        except Exception, e:
            BacnetConsoleCmd._exception("exception: %r", e)


class BacnetIPDevice(BaseDevice.Device):
	def __init__(self, type, id, address, objectName, objectIdentifier, maxApduLengthAccepted, segmentationSupported, vendorIdentifier):
		super(BacnetIPDevice,self).__init__(type, id, params)
		self.address = address
		self.objectName = objectName
		self.objectIdentifier = objectIdentifier
		self.maxApduLengthAccepted = maxApduLengthAccepted
		self.segmentationSupported = segmentationSupported
		self.vendorIdentifier = vendorIdentifier
		debug_mesg("Created BacnetIPDevice with id: "+id)
		
	def get_device_channels(self):
		pass

	def get_sample(self):
		pass
		
	def run(self):
		try:
			self.thisDevice = LocalDeviceObject(self.objectName,self.objectIdentifier,self.maxApduLengthAccepted
			self.segmentationSupported,self.vendorIdentifier)
			# build a bit string that knows about the bit names
			pss = ServicesSupported()
			pss['whoIs'] = 1
			pss['iAm'] = 1
			pss['readProperty'] = 1
			pss['writeProperty'] = 1
			# set the property value to be just the bits
			self.thisDevice.protocolServicesSupported = pss.value
			self.thisApplication = BacnetApplication(self.thisDevice, address)
			self.thisConsole = BacnetConsoleCmd()
			_log.debug("running")
			bacpypes.core.run()
		except Exception, e:
			logging.error("error %s in creating device %s:%s"%(e,self.type,self,id))
		finally:
			pass
		
		
