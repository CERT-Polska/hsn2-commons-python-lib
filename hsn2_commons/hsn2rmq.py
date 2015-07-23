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

from random import sample
import logging
import multiprocessing
import string

import pika
logging.getLogger("pika").setLevel(logging.WARNING)

from hsn2_commons.hsn2bus import Bus
from hsn2_commons.hsn2bus import BusException
from hsn2_commons.hsn2bus import BusTimeoutException
from hsn2_commons.hsn2bus import MismatchedCorrelationIdException
import time


class NoAppIdException(Exception):
    pass


class RabbitMqBus(Bus):
    host = "127.0.0.1"
    port = 5672
    connection = None

    channelFw = None
    channelOs = None
    exchange = ''
    fw_queue = 'fw:l'
    os_queue = 'os:l'
    resp_queue = None
    app_id = None
    corr_id = None

    def __init__(self, host="127.0.0.1", port=5672, app_id=None):
        '''
        @param host: address where the bus is located
        @param port: port on which the bus is available
        @param app_id: the name of the service using the adapter. Used for recognizing the console.
        '''
        self.host = host
        self.port = 5672 if port is None else int(port)
        if app_id is None:
            raise NoAppIdException
        else:
            self.app_id = app_id
        params = pika.ConnectionParameters(host=self.host, port=self.port)
        self.connection = pika.BlockingConnection(params)
        self.openChannels()

    def _wait_for_response(self, queue, on_response):
        '''
        Wait for a message to appear on the queue.
        @param queue: The queue to monitor
        @param on_response: will be run, when message received.
        @return: a tuple (method, properties, body)
        '''
        def _pre_response(ch, method, properties, body):
            if type(body) == unicode:
                data = bytearray(body, "utf-8")
                body = bytes(data)
            return on_response(ch, method, properties, body)
        self.channelFw.basic_consume(_pre_response, queue)

    def _timeout_callback(self):
        '''
        Timeout callback
        '''
        self.connection._timeouts = {}
        raise BusTimeoutException

    def openChannels(self):
        '''
        Connects to the bus.
        '''
        if logging.getLogger() is None:
            print "starting with connection... %s:%d" % (self.host, self.port)
        else:
            logging.info("Attempting to connect to %s:%d" %
                         (self.host, self.port))
        try:
            self.channelFw = self.connection.channel()
            self.channelOs = self.connection.channel()
            self.channelFw.basic_qos(prefetch_count=1)
            self.channelOs.basic_qos(prefetch_count=1)
            result = self.channelOs.queue_declare(
                durable=False, exclusive=True, auto_delete=True)
            self.resp_queue = result.method.queue
        except Exception as e:
            logging.exception(e)
            raise BusException("Can't connect to RabbitMQ")

    def sendCommand(self, dest, mtype, command, sync=0, timeout=0):
        '''
        Send a command over the bus.
        @param dest: The name of the destination. Only "fw" and "os" are supported.
        @param mtype: The message type written as a string.
        @param command: The message that is to be sent.
        @param sync: Whether to wait for a reply. 1 = True/0 = False
        @param timeout: How long to wait for a reply. Only used if sync = 1.
        @return: A tuple containing the message type as a string and the message body in that order.
        '''
        self.corr_id = None
        if dest == "fw":
            routing_key = self.fw_queue
            channel = self.channelFw
        elif dest == "os":
            routing_key = self.os_queue
            channel = self.channelOs
        else:
            raise Exception("Unknown destination: %s" % str(dest))

        if sync is 1:
            resp_queue = self.resp_queue
            if self.corr_id is None:
                self.corr_id = "%s-%s" % (mtype,
                                          ''.join(sample(string.digits, 10)))
        else:
            resp_queue = None

        channel.basic_publish(
            exchange=self.exchange,
            routing_key=routing_key,
            properties=pika.BasicProperties(
                type=str(mtype),
                content_type="application/hsn2+protobuf",
                app_id=self.app_id,
                reply_to=resp_queue,
                correlation_id=self.corr_id),
            body=None if command is "" else command.SerializeToString()
        )

        if sync is 1:
            properties = None
            wait_start = time.time()
            while True:
                method, properties, body = channel.basic_get(queue=resp_queue)
                if properties:
                    break
                if time.time() - wait_start > timeout:
                    raise BusTimeoutException()
            
            return self.on_response(channel, method, properties, body)

    def on_response(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self.corr_id and self.corr_id != properties.correlation_id and self.app_id != "cli":
            raise MismatchedCorrelationIdException(
                "Sent:%s, Received:%s" % (self.corr_id, properties.correlation_id))
        self.mtype = properties.type
        if type(body) == unicode:
            data = bytearray(body, "utf-8")
            body = bytes(data)
        self.body = body
        return properties.type, body

    def close(self):
        '''
        Closes the connection with the bus.
        '''

        if self.connection is not None:
            self.connection.close()
        self.connection = None
        self.channelFw = None
        self.channelOs = None

    def setFWQueue(self, queue):
        self.fw_queue = queue

    def attachToMonitoring(self, callback, monitoring='notify'):
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

        channel = self.connection.channel()
        channel.basic_qos(prefetch_count=1)
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue
        consumer = RabbitMqConsumer(callback)
        channel.queue_bind(exchange=monitoring,
                           queue=queue_name)
        channel.basic_consume(consumer.consume, queue=queue_name)
        channel.start_consuming()


class RabbitMqConsumer(object):

    def __init__(self, callback):
        self.callback = callback

    def consume(self, ch, method, props, body):
        success = self.callback(props.type, body)
        if (success):
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            ch.basic_reject(delivery_tag=method.delivery_tag)
