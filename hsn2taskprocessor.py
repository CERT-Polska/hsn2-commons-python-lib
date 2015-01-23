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

import sys
sys.path.append("/opt/hsn2/python/proto")
sys.path.append("/opt/hsn2/python/commlib")
from time import sleep
import signal
import logging
from multiprocessing import Process
from hsn2bus import Bus
from hsn2bus import BusTimeoutException
from hsn2bus import BusException
from hsn2bus import MismatchedCorrelationIdException
from Process_pb2 import TaskRequest
from Process_pb2 import TaskAccepted
from Process_pb2 import TaskError
from Process_pb2 import TaskCompleted
from pika.exceptions import AMQPError

from hsn2osadapter import ObjectStoreException, HSN2ObjectStoreAdapter
from hsn2dsadapter import DataStoreException, HSN2DataStoreAdapter
import hsn2objectwrapper as ow
from hsn2objectwrapper import BadValueException
import hsn2enumwrapper as enumwrap

'''
Created on 23-03-2012

@author: wojciechm
'''
class ProcessingException(Exception):
	pass

class InputException(BadValueException):
	pass

class ParamException(Exception):
	pass

class TerminationException(Exception):
	pass

class BadTypeException(Exception):
	pass

class HSN2TaskProcessor(Process):
	'''
	Template for task processing workers for HSN2 Python services.
	Using Process instead of thread to overcome CPython Global Interpreter Lock.
	'''
	keepRunning = True
	fwBus = None
	osAdapter = None
	dsAdapter = None
	datastore = None
	serviceQueue = None
	currentTask = None
	objects = None
	newObjects = None
	lastMsg = None

	def __init__(self, connector, datastore, serviceName, serviceQueue, objectStoreQueue, **extra):
		'''
		Runs Process init first and then creates required connections.
		@param connector: bus address
		@param connectorPort: bus port
		@param datastore: HSN 2 Data Store address
		@param serviceName: The name of the running service.
		@param serviceQueue: The queue the service should connect to.
		@param objectStoreQueue: The queue used for sending objects to the object store.
		'''
		Process.__init__(self)
		self.serviceName = serviceName
		self.serviceQueue = serviceQueue
		connectorPort = extra.get('connectorPort', 5672)
		self.fwBus = Bus.initBus(host = connector, port = connectorPort, app_id = serviceName)
		self.fwBus.os_queue = objectStoreQueue
		self.osAdapter = HSN2ObjectStoreAdapter(bus = self.fwBus)
		self.dsAdapter = HSN2DataStoreAdapter(datastore)

	def run(self):
		'''
		Main method of the task processor.
		Contains the main loop:
			1. receive a task
			2. receive it's object
			3. accept the task
			4. process the task
			5. update it's object
			6. complete the task
		'''
		signal.signal(signal.SIGTERM, self.sigTerm)

		while self.keepRunning:
			try:
				self.taskReceive()
			except (BusException, AMQPError) as (e):
				self.lastMsg = e.message
				logging.exception("EXCEPTION - %s - %s" % (e.__class__, e))
				break
			except Exception as e:
				self.lastMsg = e.message
				logging.exception("EXCEPTION - %s - %s" % (e.__class__, e))
				break
			except:
				self.lastMsg = "Uncaught exception"
				logging.exception("Uncaught exception")
				break
			
		self.fwBus.close()
		self.cleanup()

	def process(self, ch, method, properties, body):
		try:
			if properties.type != "TaskRequest":
				raise BadTypeException(properties.type)
			tr = TaskRequest()
			tr.ParseFromString(body)
			self.currentTask = tr
			self.newObjects = []
			self.taskAccept()
			self.objects = self.osAdapter.objectsGet(self.currentTask.job, [self.currentTask.object])
			warnings = self.taskProcess()
			if warnings is None:
				warnings = list()
			self.osAdapter.objectsUpdate(self.currentTask.job, self.objects, overwrite = True)
			self.taskComplete(warnings)
			self.taskClear()
			ch.basic_ack(delivery_tag = method.delivery_tag)
		except MismatchedCorrelationIdException as (e):
			self.taskError('DEFUNCT', 'Mismatched correlation ids: %s.' % e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except BadTypeException as (mtype):
			self.taskError('DEFUNCT', 'Bad message type received %s.' % mtype)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except ParamException as (e):
			self.taskError('PARAMS', e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except ObjectStoreException as (e):
			self.taskError('OBJ_STORE', e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except DataStoreException as (e):
			self.taskError('DATA_STORE', e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except ProcessingException as (e):
			self.taskError('DEFUNCT', e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()
		except InputException as (e):
			self.taskError('INPUT', e.message)
			ch.basic_ack(delivery_tag = method.delivery_tag)
			self.taskClear()

	def taskReceive(self):
		'''
		Receive a task from the service queue and assign it to the current task.
		'''
		self.fwBus._wait_for_response(self.serviceQueue, self.process)
		

	def taskAccept(self):
		'''
		Sends the TaskAccept message for the current task.
		'''
		ta = TaskAccepted()
		ta.task_id = self.currentTask.task_id
		ta.job = self.currentTask.job
		logging.info("Task accepted - tid %s, jid %s" % (self.currentTask.task_id, self.currentTask.job))
		self.fwBus.sendCommand("fw", "TaskAccepted", ta, False)
		return True

	def taskProcess(self):
		'''	
		This method should be overridden with what is to be performed.
		The current task is available at self.currentTask.
		@return: A list of warnings. 
		'''
		self.objects[0].addFlag("Nice")
		self.objects[0].addTime("Pork", 111)
		self.objects[0].removeAttribute("Pork")
		try:
			print '''Printing hosts file'''
			print self.dsAdapter.getFile(self.currentTask.job, self.objects[0].hosts.key)
		except Exception:
			print '''No hosts file attached to object'''
		obj = ow.Object()
		obj.addFlag("Bad")
		obj.addBytes('hosts', self.dsAdapter.putFile("/etc/hosts", self.currentTask.job))
		obj.addObject("parent", self.objects[0].getObjectId())
		self.osAdapter.objectsPut(self.currentTask.job, self.currentTask.task_id, [obj])
		sleep(1) #simulate processing
		return []

	def taskComplete(self, warnings = []):
		'''
		Send the TaskComplete message for the current task.
		@param warnings: list of warnings generated while processing.
		'''
		tc = TaskCompleted()
		tc.task_id = self.currentTask.task_id
		tc.job = self.currentTask.job
		for w in warnings:
			tc.warnings.append(w)
		for obj in self.newObjects:
			tc.objects.append(obj)
		logging.debug("New objects:" + str(tc.objects))
		logging.debug("Warnings:" + str(tc.warnings))
		logging.info("Task completed - tid %s, jid %s" % (self.currentTask.task_id, self.currentTask.job))
		self.fwBus.sendCommand("fw", "TaskCompleted", tc, False)
		logging.info("Complete sent")
		return True

	def taskError(self, reason = 'DEFUNCT', desc = ""):
		'''
		Send the TaskError message for the current task.
		@param reason: The reason which is to be reported. Needs to be a name from the enum list.
		@param desc: String description of the error.
		'''
		te = TaskError()
		te.task_id = self.currentTask.task_id
		te.job = self.currentTask.job
		te.reason = enumwrap.getValue(te, "ReasonType", reason)
		te.description = desc
		logging.warning(reason + " " + desc)
		self.fwBus.sendCommand("fw", "TaskError", te, False)
		return True

	def taskClear(self):
		'''
		Clears information left over by previously processed task.
		'''
		self.currentTask = None
		self.newObjects = None
		self.objects = None

	def cleanup(self):
		'''
		This method is meant to be overridden.
		It is called in the shutdown procedure and should contain all relevant cleanup calls. 
		'''
		pass

	def sigTerm(self, sig, stack):
		'''
		Defines how the taskProcessor should handle certain signals.
		Responsible for changing the keepRunning flags.
		@param arrived: The signal that arrived.
		@param stack: The current call stack.
		'''
		if sig == signal.SIGTERM:
			logging.debug("Received sig term.")
			self.keepRunning = False
			self.osAdapter.keepRunning = False

	def paramToBool(self, param):
		'''
		Method used for converting received parameter values to their boolean form.
		@param param: The parameter that is to have it's value converted.
		@return a boolean value.
		'''
		try:
			return ow.toBoolValue(param.value)
		except BadValueException:
			raise ParamException("Parameter '%s' has an incorrect value." % param.name)
