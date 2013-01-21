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
import sys
sys.path.append("/opt/hsn2/python/proto")
sys.path.append("/opt/hsn2/python/commlib")
from Object_pb2 import ObjectData
from Resources_pb2 import JSContextList, BehaviorsList, YaraMatch, YaraMatchesList, IpAddresses, DnsQueries, ProtocolEntries
from Resources_pb2 import SingleCheckerVerdict, DomainVerdict, DomainVerdicts
import hsn2enumwrapper as enumwrap
import logging

class BadValueException(ValueError):
	pass

types = {
	'EMPTY':None,
	'BOOL':'data_bool',
	'INT':'data_int',
	'FLOAT':'data_float',
	'TIME':'data_time',
	'STRING':'data_string',
	'BYTES':'data_bytes',
	'OBJECT':'data_object'
	}

def toObjects(several):
	'''
	Process a list of objects from the external format to the internal one.
	@param several: List of external objects to be converted.
	@return: List of internal objects.
	'''
	retObjs = list()
	for item in several:
		retObjs.append(toObject(item))
	return retObjs

def fromObjects(several):
	'''
	Process a list of objects from the internal format to the external one.
	@param several: List of internal objects to be converted.
	@return: List of external objects.
	'''
	retObjs = list()
	for item in several:
		retObjs.append(fromObject(item))
	return retObjs

class Reference():
	'''
	Class for storing references to Data Store objects.
	'''
	def __init__(self, key, store):
		self.key = key
		self.store = store

	def getKey(self):
		return self.key

	def setKey(self, key):
		self.key = key

	def getStore(self):
		return self.store

	def setStore(self, store):
		self.store = store

	def getBoth(self):
		return (self.key, self.store)

	def setBoth(self, key, store):
		(self.key, self.store) = (key, store)

	def __str__(self):
		return str(self.getStore()) + "|" + str(self.getKey())

class Object():
	'''
	Class for internal representation of HSN2 objects.
	'''
	internalStoreType = None
	internalStoreId = None

	def __init__(self, ident = None):
		self.internalStoreId = ident
		self.internalStoreType = dict()

	def setObjectId(self, ident):
		self.internalStoreId = ident

	def getObjectId(self):
		return self.internalStoreId

	def setType(self, name, hsn2type):
		self.internalStoreType[name] = hsn2type

	def getTypeStore(self):
		return self.internalStoreType

	def isSet(self, name):
		if self.internalStoreType.get(name) is None:
			return False
		return True

	def addAttribute(self, hsn2type, name, value):
		'''
		Adding an attribute which is already set will replace the previous type/value.
		'''
		setattr(self, name, value)
		self.internalStoreType[name] = hsn2type

	def addFlag(self, name):
		self.addAttribute("EMPTY", name, None)

	def addBool(self, name, value):
		value = toBoolValue(value)
		self.addAttribute("BOOL", name, value)

	def addInt(self, name, value):
		self.addAttribute("INT", name, int(value))

	def addFloat(self, name, value):
		self.addAttribute("FLOAT", name, float(value))

	def addTime(self, name, value):
		self.addAttribute("TIME", name, value)

	def addString(self, name, value):
		if type(value) == unicode:
			self.addAttribute("STRING", name, value)
		else:
			self.addAttribute("STRING", name, str(value))

	def addBytes(self, name, keyValue, storeValue = None):
		if storeValue is not None: storeValue = int(storeValue)
		ref = Reference(long(keyValue), storeValue)
		self.addAttribute("BYTES", name, ref)

	def addBytes2(self, name, keyValue, storeValue = None):
		'''
		Same as addBytes, but without the cast of keyValue to long.
		Required for object-feeder functionality.
		'''
		if storeValue is not None: storeValue = int(storeValue)
		ref = Reference(keyValue, storeValue)
		self.addAttribute("BYTES", name, ref)

	def addObject(self, name, value):
		self.addAttribute("OBJECT", name, long(value))

	def removeAttribute(self, name):
		delattr(self, name)
		del(self.internalStoreType[name])

def toBoolValue(value):
	'''
	Used for converting values to boolean type.
	@param value: The value that is to be converted.
	@return: A boolean value.
	'''
	if not isinstance(value, bool):
		value = str(value).lower()
		if value == "true" or value == "1":
			value = True
		elif value == "false" or value == "0":
			value = False
		else:
			raise BadValueException("Attribute incorrect bool value: %s" % value)
	return value

def toObject(pbObject):
	'''
	Process the external format of the object to the internal one.
	In the current case this changes a Protocol Buffers message into a python object.
	@param pbObject: An object in external format.
	@return: An object in internal format.
	'''
	intObject = Object(pbObject.id)
	for attr in pbObject.attrs:
		value_type = enumwrap.getName(attr, "Type", attr.type)
		value_name = types.get(value_type)
		if value_name is not None:
			value = getattr(attr, value_name)
			if value_type == "BYTES":
				intObject.addBytes(attr.name, attr.data_bytes.key, attr.data_bytes.store)
				continue
		else: value = None
		intObject.addAttribute(value_type, attr.name, value)
	return intObject

def fromObject(intObject):
	'''
	Process the internal format of the object to the external one.
	In the current case this changes a Python object into a Protocol Buffers message.
	@param intObject: An object in internal format.
	@return: An object in external format.
	'''
	pbObject = ObjectData()
	objId = intObject.getObjectId()
	if objId is not None:
		pbObject.id = objId
	for attr_name, value_type in intObject.getTypeStore().iteritems():
		attr = pbObject.attrs.add()
		attr.name = attr_name
		attr.type = enumwrap.getValue(attr, "Type", value_type)
		value_name = types.get(value_type)
		if value_name != None:
			if value_type == "BYTES":
				ref = getattr(intObject, attr_name)
				(refKey, refStore) = ref.getBoth()
				attr.data_bytes.key = refKey
				if refStore is not None:
					attr.data_bytes.store = refStore
			else:
				setattr(attr, value_name, getattr(intObject, attr_name))
	return pbObject

def toObjectsFromJSON(jsonDump, ignoreIds = False):
	'''
	Reads objects from a hsn2-unicorn dump file.
	@param jsonDump: The unicorn dump from which objects will be loaded.
	@param ignoreIds: If True object ids will be ignored.
	@return list of objects in internal format.
	'''
	retObjs = list()
	text = open(jsonDump, 'rb').readlines()
	text = [x.strip() for x in text]
	text = ''.join(text)
	strings = ['{' + x + '}' for x in text.strip('{}').split('}{')]
	for string in strings:
		obj = toObjectFromJSON(string, ignoreIds)
		if obj is not None:
			retObjs.append(obj)
	return retObjs

def toObjectFromJSON(jsonString, ignoreIds = False):
	'''
	Converts a single object from a unicorn dump file to internal format.
	@param jsonString: The string containing the object.
	@param ignoreIds: If True the object id will be ignored.
	@return an object in internal format.
	'''
	import json
	js = json.loads(jsonString)
	attrs = js.get('attrs')
	if ignoreIds:
		intObject = Object()
	else:
		objId = js.get('id')
		if objId is None:
			return None
		intObject = Object(objId)
	if attrs is None:
		return intObject
	for attr in attrs:
		value_type = attr.get("type")
		value_name = types.get(value_type)
		if value_name is not None:
			value = attr.get(value_name)
			if value_type == "BYTES":
				intObject.addBytes2(attr.get('name'), value.get('key'), value.get('store'))
				continue
		else: value = None
		intObject.addAttribute(value_type, attr.get('name'), value)
	return intObject

def toIpAddressList(ipList):
	iList = IpAddresses()
	iList.ip.extend(ipList)
	return iList

def fromIpAddressList(fileP):
	ipList = IpAddresses()
	ipList.ParseFromString(fileP.read())
	fileP.close()
	nList = []
	for ip in ipList.ip:
		nList.append(ip)
	return nList

def toDnsList(dnsList):
	dList = DnsQueries()
	dList.dns.extend(dnsList)
	return dList


def fromDnsList(fileP):
	dnsList = DnsQueries()
	dnsList.ParseFromString(fileP.read())
	fileP.close()
	nList = []
	for dns in dnsList.dns:
		nList.append(dns)
	return nList

def toFilterList(filterList):
	fList = ProtocolEntries()
	fList.frame.extend(filterList)
	return fList

def toObjectDomainVerdicts(result):
	domainVerdicts = DomainVerdicts()
	for verd in result:
		domainVerdict = domainVerdicts.verdict.add()
		domainVerdict.checked_domain = result[verd].getCheckedDomain()
		singleList = result[verd].getSingleVerdicts()
		for singleVerd in singleList:
			single = domainVerdict.checked.add()
			single.name = singleVerd.getName()
			single.verdict = singleVerd.getVerdict()
	return domainVerdicts

def fromObjectDomainVerdicts(byteObject):
	result = {}
	domainVerdicts = DomainVerdicts()
	domainVerdicts.ParseFromString(byteObject)
	for verd in domainVerdicts.verdict:
		result[verd.checked_domain] = list()
		for single in verd.checked:
			result[verd.checked_domain].append([single.name, single.verdict])
	return result

def toBehaviorList(normalList):
	'''
	Converts a list of dictionaries to a protocol buffers BehaviorList message.
	Each dictionary should have a "description_text" key defined.
	Optionally it can have a "discovery_method" key defined.
	@param normalList: The list that is to be converted.
	@return: The protocol buffers message.
	'''
	bList = BehaviorsList()
	for behavior in normalList:
		beh = bList.behavior.add()
		beh.description_text = str(behavior.get("description_text", ""))
		val = behavior.get("discovery_method")
		if val is not None:
			beh.discovery_method = str(val)
	return bList


def fromBehaviorList(fileP):
	'''
	Converts a protocol buffers BehaviorList message to a list of dictionaries.
	Each Behavior should have a "description_text" attribute defined.
	Optionally it can have a "discovery_method" attribute defined.
	@param fileP: An object containing the message. It needs to support the read method. 
	@return: The list of dictionaries.
	'''
	bList = BehaviorsList()
	bList.ParseFromString(fileP.read())
	nList = []
	for beh in bList.behavior:
		obj = {"description_text" : beh.description_text}
		try:
			obj["discovery_method"] = getattr(beh, "discovery_method")
		except:
			pass
		finally:
			nList.append(obj)
	return nList

def toJSContextList(normalList):
	'''
	Converts a list of dictionaries to a protocol buffers JSContextList message.
	Each dictionary should have "id", "source", "eval" keys defined.
	@param normalList: The list that is to be converted.
	@return: The protocol buffers message.
	'''
	cList = JSContextList()
	for context in normalList:
		cont = cList.contexts.add()
		cont.id = int(context.get("id"))
		cont.source = str(context.get("source"))
		cont.eval = toBoolValue(context.get("eval"))
	return cList

def fromJSContextList(fileP):
	'''
	Converts a protocol buffers JSContextList message to a list of dictionaries.
	Each JSContext should have "id", "source", "eval" attributes defined.
	@param fileP: An object containing the message. It needs to support the read method. 
	@return: The list of dictionaries.
	'''
	cList = JSContextList()
	cList.ParseFromString(fileP.read())
	nList = []
	for cont in cList.contexts:
		obj = {
			"id" : cont.id,
			"source" : cont.source,
			"eval" : cont.eval
		}
		nList.append(obj)
	return nList

def toYaraMatchesList(matches):
	'''
	Converts a list of matches to a protocol buffers YaraMatchesList message.
	Each dictionary should have a "rule" key defined.
	Optionally it can have a "namespace" key defined.
	@param matches: The list that is to be converted.
	@return: The protocol buffers message.
	'''
	pbmatches = YaraMatchesList()
	for match in matches:
		newmatch = pbmatches.matches.add()
		newmatch.rule = str(match.get("rule", ""))
		namespace = match.get("namespace")
		if namespace is not None:
			newmatch.namespace = str(namespace)
	return pbmatches

def fromYaraMatchesList(pbmatches):
	'''
	Converts a protocol buffers YaraMatchesList message to a list of dictionaries.
	Each YaraMatch should have a "rule" attribute defined.
	Optionally it can have a "namespace" attribute defined.
	@param pbmatches: An object containing the message. It needs to support the read method.
	@return: The list of dictionaries.
	'''
	matches = YaraMatchesList()
	matches.ParseFromString(pbmatches.read())
	ret = []
	for match in matches.matches:
		obj = {"rule" : match.rule}
		try:
			obj["namespace"] = getattr(match, "namespace")
		except:
			pass
		finally:
			ret.append(obj)
	return ret

if __name__ == '__main__':
	obj = toObjectsFromJSON('/root/dump/1334690752502.json', True)
	print obj
	for ob in obj:
		print ob.__dict__
		if ob.isSet("content"):
			print ob.content.__dict__
	print fromObjects(obj)
