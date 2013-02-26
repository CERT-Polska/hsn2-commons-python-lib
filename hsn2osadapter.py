#!/usr/bin/python -tt

# Copyright (c) NASK, NCSC
# 
# This file is part of HoneySpider Network 2.0.
# 
# This is a free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''
Created on 05-04-2012

@author: wojciechm
'''
import logging
from hsn2bus import BadMessageException, BusTimeoutException
import hsn2enumwrapper as enumwrap
import hsn2objectwrapper as ow
from hsn2rmq import RabbitMqBus
from ObjectStore_pb2 import ObjectRequest
from ObjectStore_pb2 import ObjectResponse

class NoBusException(Exception):
	pass

class ObjectStoreException(Exception):
	pass

class QueryStructure():
	'''
	Used for defining queries to the Object Store. One query can contain several query structures.
	This is an internal format. It doesn't directly correspond to the QueryStructure from the protocol.
	For the QueryStructure from the protocol see the protocol buffers generated ObjectStore_pb2.
	'''
	def __init__(self, obj, negate = False):
		'''
		A query structure uses the internal object representation for storing attributes.
		Each attribute can only be defined once per query structure.
		To define several criteria for a single attribute use several query structures.
		A single query structure can be used to pass several different attributes.
		@param obj: The object from which the query is to take it's attributes
		@param negate: Whether the attributes are to be negated.
		'''
		self.obj = obj
		self.negate = negate

	def getAttributes(self):
		return self.obj

	def getNegate(self):
		return self.negate

	def setAttributes(self, obj):
		self.obj = object

	def setNegate(self, negate):
		self.negate = negate

class HSN2ObjectStoreAdapter(object):
	'''
	Responsible for communicating with the HSN2 object store.
	Should be initialized with a RabbitMqBus.
	'''
	bus = None
	missing = None
	maxTries = 1
	keepRunning = True
	timeout = 600

	def __init__(self, bus = None):
		'''
		Requires a working RabbitMqBus.
		@param bus:
		'''
		if bus is None:
			raise NoBusException()
		self.bus = bus

	def sendRequest(self, objReq):
		'''
		Sends a prepared ObjectRequest to the object store.
		@param objReq: A previously prepared ObjectRequest.
		@return: ObjectResponse
		'''
		waiting = True
		tries = 1
		objResp = None
		while waiting and self.keepRunning:
			try:
				(mtype, reponse) = self.bus.sendCommand("os", "ObjectRequest", objReq, sync = 1, timeout = self.timeout)
				if mtype != "ObjectResponse":
					raise BadMessageException("ObjectResponse", mtype)
				#TODO: should implement appropriate mechanisms for various responses.
				objResp = ObjectResponse()
				objResp.ParseFromString(reponse)
				if enumwrap.getValue(objResp, "ResponseType", objResp.type) == "FAILURE":
					logging.error("Failed ObjectRequest: " + str(objResp))
					break
				logging.debug(objResp)
				waiting = False
			except BusTimeoutException:
				if tries >= self.maxTries:
					raise ObjectStoreException("Object store not responding. Tried %d times." % tries)
				else: tries = tries + 1
				logging.info("ObjectRequest reply not received yet. Resending request")
				pass
		if objResp is None:
			raise ObjectStoreException("Termination of service while requesting objects.")
		return objResp

	def objectsGet(self, jobId, objects):
		'''
		Retrieve the objects by their ids.
		@param jobId The id of the job to which the objects belong
		@param objects A list of objects Ids to fetch. List as in [].
		@return: list of retrieved objects in the internal format.
		'''
		logging.debug("Performing ObjectRequest GET request")
		if len(objects) == 0:
			logging.debug("No objects passed to objectsGet")
			return None
		objReq = ObjectRequest()
		objReq.job = jobId
		objReq.type = enumwrap.getValue(objReq, "RequestType", "GET")
		for obj in objects:
			objReq.objects.append(obj)
		logging.info("requesting objects" + str(objects))
		objResp = self.sendRequest(objReq)
		self.missing = objResp.missing
		currentObjects = ow.toObjects(objResp.data)
		return currentObjects

	def objectsUpdate(self, jobId, objects, overwrite = False):
		'''
		Update objects in the object store.
		@param jobId The id of the job to which the objects belong
		@param objects The list of objects (internal format) which were modified.
		@param overwrite Whether to overwrite previously set attributes [default=False]
		'''
		logging.debug("Performing ObjectRequest UPDATE request")
		if len(objects) == 0: return None
		objReq = ObjectRequest()
		objReq.job = jobId
		objReq.type = enumwrap.getValue(objReq, "RequestType", "UPDATE")
		objReq.overwrite = overwrite
		logging.debug("Objects being updated:")
		logging.debug(objects)
		for obj in ow.fromObjects(objects):
			objData = objReq.data.add()
			objData.CopyFrom(obj)
		logging.debug(objReq)
		self.sendRequest(objReq)

	def objectsPut(self, jobId, taskId, objects, raw = False):
		'''
		Update objects in the object store.
		@param jobId The id of the job to which the objects belong
		@param taskId The id of the task to which the objects belong
		@param objects The list of objects (internal format) which were added
		@param raw Whether this is supposed to be a raw put (used in imports)
		@return: List of object ids.
		'''
		logging.debug("Performing ObjectRequest PUT request")
		if len(objects) == 0: return None
		objReq = ObjectRequest()
		objReq.job = jobId
		objReq.task_id = taskId
		if raw:
			objReq.type = enumwrap.getValue(objReq, "RequestType", "PUT_RAW")
		else:
			objReq.type = enumwrap.getValue(objReq, "RequestType", "PUT")
		logging.debug("Objects being added:")
		logging.debug(objects)
		for obj in ow.fromObjects(objects):
			objData = objReq.data.add()
			objData.CopyFrom(obj)
		logging.debug(objReq)
		objResp = self.sendRequest(objReq)
		return objResp.objects

	def query(self, jobId, queryStructs = list()):
		'''
		Query the object store for objects with the filters specified by attributes
		@param jobId: The id of the job to which the query is addressed
		@param queryStructs: A list of queryStructures.
		'''
		objReq = ObjectRequest()
		objReq.job = jobId
		objReq.type = enumwrap.getValue(objReq, "RequestType", "QUERY")

		for qS in queryStructs:
			attributes = qS.getAttributes()
			negate = qS.getNegate()
			pbObj = ow.fromObject(attributes)
			for attr in pbObj.attrs:
				queryObj = objReq.query.add()
				val = getattr(attributes, attr.name)
				queryObj.attr_name = attr.name
				queryObj.negate = negate
				if val is None:
					queryObj.type = enumwrap.getValue(queryObj, "QueryType", "BY_ATTR_NAME")
				else:
					queryObj.type = enumwrap.getValue(queryObj, "QueryType", "BY_ATTR_VALUE")
					queryObj.attr_value.CopyFrom(attr)
		objResp = self.sendRequest(objReq)
		return objResp.objects

if __name__ == '__main__':
	fwBus = RabbitMqBus(host = "localhost",
		port = 5672,
		resp_queue = "service:l",
		app_id = "osTest")
	fwBus.openFwChannel()
	osCon = HSN2ObjectStoreAdapter(fwBus)
	print osCon.query(165)
	obj = ow.Object()
	obj.addFlag("Bad")
	obj2 = ow.Object()
	obj2.addObject("parent", 1)
	qS = QueryStructure(obj, False)
	qS2 = QueryStructure(obj2, False)
	print osCon.query(165, [qS]), "containing flag Bad"
	qS.setNegate(True)
	print osCon.query(165, [qS]), "not containing flag Bad"
	qS.setNegate(False)
	print osCon.query(165, [qS2]), "have parent 1"
	qS2.setNegate(True)
	print osCon.query(165, [qS2]), "don't have parent 1"
	qS2.setNegate(False)
	print osCon.query(165, [qS, qS2]), "containing flag Bad and have parent 1"
	qS2.setNegate(True)
	print osCon.query(165, [qS, qS2]), "containing flag Bad and don't have parent 1"
	for obj in osCon.objectsGet(165, osCon.query(165, [qS2])):
		print obj.__dict__
