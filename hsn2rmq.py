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

import pika
from random import sample
import time
from pika.spec import Basic
import string
import logging

from hsn2bus import Bus
from hsn2bus import BusException
from hsn2bus import BusTimeoutException
from hsn2bus import MismatchedCorrelationIdException

class NoAppIdException(Exception):
	pass

class RabbitMqBus(Bus):
	host = "127.0.0.1"
	port = 5672
	connection = None
	channel = None
	exchange = ''
	timeout = 10
	fw_queue = 'fw:l'
	os_queue = 'os:l'
	app_id = None

	def __init__(self, host = "127.0.0.1", port = 5672, app_id = None):
		'''
		@param host: address where the bus is located
		@param port: port on which the bus is available
		@param app_id: the name of the service using the adapter. Used for recognizing the console.
		'''
		self.host = host
		self.port = 5672 if port == None else int(port)
		if app_id is None:
			raise NoAppIdException
		else:
			self.app_id = app_id

	def _wait_for_response(self, queue, timeout = 120):
		'''
		Wait for a message to appear on the queue.
		@param queue: The queue to monitor
		@param timeout: How long to wait in seconds.
		@return: a tuple (method, properties, body)
		'''
		sleep_interval = 0.1
		start_time = time.time()

		self.connection.add_timeout(timeout, self._timeout_callback)		
		while (time.time() - start_time) < timeout:
			reply = self.channel.basic_get(0, queue, no_ack = True)
			if isinstance(reply[0], Basic.GetOk):
				self.connection._timeouts = {}
				return reply
			time.sleep(sleep_interval)
		raise BusTimeoutException

	def _timeout_callback(self):
		'''
		Timeout callback
		'''
		self.connection._timeouts = {}
		raise BusTimeoutException

	def openFwChannel(self):
		'''
		Connects to the bus.
		'''
		if logging.getLogger() is None:
			print "starting with connection... %s:%d" % (self.host, self.port)
		else:
			logging.info("Attempting to connect to %s:%d" % (self.host, self.port))
		params = pika.ConnectionParameters(
				host = self.host,
				port = self.port)
		try:
			self.connection = pika.BlockingConnection(params)
			self.channel = self.connection.channel()
			self.channel.basic_qos(None, 0, 1, False)
		except:
			raise BusException("Can't connect to RabbitMQ")

	def sendCommand(self, dest, mtype, command, sync = 0, timeout = 0, corr_id = None):
		'''
		Send a command over the bus.
		@param dest: The name of the destination. Only "fw" and "os" are supported.
		@param mtype: The message type written as a string.
		@param command: The message that is to be sent.
		@param sync: Whether to wait for a reply. 1 = True/0 = False
		@param timeout: How long to wait for a reply. Only used if sync = 1.
		@param corr_id: The correlation id to use. If None then it will be generated.
		@return: A tuple containing the message type as a string and the message body in that order.
		'''
		if dest == "fw":
			routing_key = self.fw_queue
		elif dest == "os":
			routing_key = self.os_queue
		else:
			raise Exception("Unknown destination: %s" % str(dest))

		if timeout <= 0:
			timeout = self.timeout
		if self.channel is None:
			raise BusException

		if sync is 1:
			result = self.channel.queue_declare(
				durable = False, exclusive = True, auto_delete = True)
			resp_queue = result.method.queue
			if corr_id is None:
				corr_id = "%s-%s" % (mtype, ''.join(sample(string.digits, 10)))
		else:
			resp_queue = None
		
		self.channel.basic_publish(
			exchange = self.exchange,
			routing_key = routing_key,
			properties = pika.BasicProperties(
				type = str(mtype),
				content_type = "application/hsn2+protobuf",
				app_id = self.app_id,
				reply_to = resp_queue,
				correlation_id = corr_id),
			body = None if command is "" else command.SerializeToString());
		

		if sync is 1:
			(method, properties, body) = self._wait_for_response(resp_queue, timeout)
			if corr_id != properties.correlation_id and self.app_id != "cli":
				raise MismatchedCorrelationIdException("Sent:%s, Received:%s" % (corr_id, properties.correlation_id))
			mtype = properties.type
			self.channel.queue_delete(queue = resp_queue)
			return (mtype, body)

	def close(self):
		'''
		Closes the connection with the bus.
		'''
		if self.connection is not None:
			self.connection.close()
		self.connection = None
		self.channel = None

	def setFWQueue(self, queue):
		self.fw_queue = queue

	def attachToMonitoring(self, callback, monitoring = 'notify'):
	    '''
	    Example:
	            ...
	            def start(self):
	                    bus = Bus.createConfigurableBus(self.logger, self.config, 'some-app-name')
	                    bus.openFwChannel()
	                    bus.attachToQueue("some-queue-name", self.consume)
	            def consume(self, type, body):
	                            print "[X] consuming... %s" % type
	    '''
	    result = self.channel.queue_declare(exclusive=True)
	    queue_name = result.method.queue
	    consumer = RabbitMqConsumer(callback)
	    self.channel.queue_bind(exchange=monitoring,
                       queue=queue_name)
	    self.channel.basic_consume(consumer.consume, queue=queue_name)
	    self.channel.start_consuming()

class RabbitMqConsumer(object):

	def __init__(self, callback):
		self.callback = callback

	def consume(self, ch, method, props, body):
		success = self.callback(props.type, body)
		if (success):
				ch.basic_ack(delivery_tag = method.delivery_tag)
		else:
				ch.basic_reject(delivery_tag = method.delivery_tag)


