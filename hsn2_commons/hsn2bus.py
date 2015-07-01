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

import ConfigParser
import sys


class BusException(Exception):
    pass


class BusTimeoutException(Exception):
    pass


class BadMessageException(Exception):
    expected = None
    got = None

    def __init__(self, expected, got):
        self.expected = expected
        self.got = got

    def __str__(self):
        return repr("Expected '%s', but got '%s'" % (self.expected, self.got))


class MismatchedCorrelationIdException(Exception):
    '''
    Raised when the response contains a different correlation id than the one which was sent.
    '''
    pass


class Bus(object):
    "Abstract Bus class"

    def openChannels(self):
        '''
        Connects to the bus.
        '''
        raise Exception(
            "This method need to be implemented in an appropriate class!")

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
        raise Exception(
            "This method need to be implemented in an appropriate class!")

    def _wait_for_response(self, queue, timeout=120):
        '''
        Wait for a message to appear on the queue.
        @param queue: The queue to monitor
        @param timeout: How long to wait in seconds.
        @return: a tuple (method, properties, body)
        '''
        raise Exception(
            "This method need to be implemented in an appropriate class!")

    def close(self):
        '''
        Closes the connection with the bus.
        '''
        raise Exception(
            "This method need to be implemented in an appropriate class!")

    def setFWQueue(self, queue):
        '''
        Sets the queue name (with priority notation) to be used for connecting to the framework.
        '''
        raise Exception(
            "This method need to be implemented in an appropriate class!")

    @staticmethod
    def createConfigurableBus(logger, config, app_id='unknown'):
        '''
        Intializes the bus adapter with configuration parameters loaded from the supplied configuration file.
        This method is used solely by the management components (HSN2 Console + WebGUI).
        @param logger: A Logger object.
        @param config: A ConfigParser object containing the loaded configuration file.
        @return: Bus object
        '''
        busName = config.get("core", "mq")
        if busName == "rabbitmq":
            try:
                rmqHost = config.get("rabbitmq", "server")
            except ConfigParser.NoOptionError:
                print "rabbitmq:server is required parameter!"
                sys.exit(1)
            try:
                rmqPort = config.getint("rabbitmq", "port")
            except ConfigParser.NoOptionError:
                rmqPort = None
            logger.debug("Bus configuration is: %s:%d", rmqHost, rmqPort)
            bus = Bus.initBus(rmqHost, rmqPort, app_id=app_id)
            bus.setFWQueue("fw:h")
            return bus
        else:
            raise Exception("Unknown mq implementation: %s" % str(busName))

    @staticmethod
    def initBus(host="127.0.0.1", port=5672, app_id=None):
        '''
        Creates, initalizes and returns the bus adapter object.
        @param host: address where the bus is located
        @param port: port on which the bus is available
        @param app_id: the name of the service using the adapter. Used for recognizing the console.
        '''
        from hsn2_commons.hsn2rmq import RabbitMqBus
        return RabbitMqBus(host=host, port=port, app_id=app_id)
