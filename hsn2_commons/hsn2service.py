#!/usr/bin/python -tt

# Copyright (c) NASK, NCSC
#
# This file is part of HoneySpider Network 2.1.
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

from multiprocessing.process import active_children
import time
import logging
import signal
import sys
import os

from hsn2_commons import argparsealiases as argparse


class NoTaskProcessorException(Exception):
    pass


class HSN2Service(object):
    '''
    This is a base class used for creating new python based services for the HSN2 project.
    '''
    serviceName = "service"
    serviceQueue = None  # has to be assigned in constructor
    connector = "127.0.0.1"
    connectorPort = 5672
    datastore = "localhost:8080"
    objectStoreQueue = "os:l"
    maxThreads = 1
    processList = None
    keepRunning = True
    nugget = None
    taskProcessorClass = None
    description = 'HSN2 Python service template.'

    def __init__(self, taskProcessorClass):
        '''
        Assigns the processor class that this service will use.
        '''
        self.processList = []
        self.taskProcessorClass = taskProcessorClass

    def standardOptions(self):
        '''
        Defines the standard command line options for services.
        '''
        parser = argparse.ArgumentParser(
            description=self.description,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument('-l', '--log-level', action='store', help='logging level (default: WARN)',
                            choices='DEBUG INFO WARN ERROR'.split(), default="WARN", dest='logLevel')
        parser.add_argument('--maxThreads', '-T', action='store', help='maximum number of threads',
                            type=int, default=self.maxThreads, dest='maxThreads')
        parser.add_argument('--connector', '-c', action='store', help='connector address',
                            default=self.connector, dest='connector')
        parser.add_argument('--connector-port', '-p', action='store', help='connector port',
                            type=int, default=self.connectorPort, dest='connectorPort')
        parser.add_argument('--datastore', '-d', action='store', help='datastore address',
                            default=self.datastore, dest='datastore')
        parser.add_argument('--service-name', '-s', action='store', help='service name',
                            default=self.serviceName, dest='serviceName')
        parser.add_argument('--service-queue-dest', '-q', action='store', help='service queue name',
                            default="", dest='serviceQueue')
        parser.add_argument('--object-store-queue-name', '-o', action='store', help='object store queue name',
                            default=self.objectStoreQueue, dest='objectStoreQueue')
        return parser

    def extraOptions(self, parser):
        '''
        This method which should be overridden in order to provide additional options for the service.
        @param parser: The parser that is to be modified.
        return The modified parser.
        '''
        return parser

    def cliparse(self):
        '''
        Parses the command line options.
        @return object containing parsed command line options.
        '''
        parser = self.standardOptions()
        parser = self.extraOptions(parser)
        args = parser.parse_args()
        if len(args.serviceQueue) == 0:
            args.serviceQueue = "srv-%s:l" % args.serviceName
        return args

    def sanityChecks(self, cliargs):
        '''
        All possible checks on CLI arguments should be performed here (ex. checking if file exists)
        @return: True if all the checks passed and service can startup.
        '''
        return True

    def start(self, params):
        '''
        Starts the task processors.
        @param params: Paramters which are to be passed to the task processors.
        '''
        index = 0
        if self.taskProcessorClass is None:
            raise NoTaskProcessorException()
        maxThreads = params.__dict__.pop('maxThreads')
        self.serviceName = params.__dict__.get('serviceName')
        try:
            while index < maxThreads:
                process = self.taskProcessorClass(**params.__dict__)
                self.processList.append(process)
                index += 1
            for process in self.processList:
                process.start()
        except Exception as e:
            logging.error(e.message)
            logging.error(e.getStackTrace())
            raise

    def run(self):
        '''
        Contains the main loop responsible for watching over it's task processors.
        Mostly sleeps unless all task processors fail.
        '''
        while(self.keepRunning):
            time.sleep(1)
            if len(active_children()) == 0 and self.keepRunning:
                logging.error("All children exited.")
                self.keepRunning = False

    def stop(self):
        '''
        Handles stopping active task processors.
        '''
        for p in self.processList:
            p.terminate()
        stop_started = time.time()
        while active_children():
            logging.debug("Waiting for children to stop")
            if time.time() - stop_started > 10:
                for p in self.processList:
                    os.kill(p.pid, signal.SIGKILL)
            time.sleep(1)
        self.processList = []
        logging.info("Service %s stopped" % self.serviceName)

    def signalHandler(self, arrived, stack):
        '''
        Defines how the service should handle certain signals. Responsible for changing the keepRunning flag.
        @param arrived: The signal that arrived.
        @param stack: The current call stack.
        '''
        logging.debug("Received signal:" + str(arrived))
        if arrived == signal.SIGINT or arrived == signal.SIGTERM:
            self.keepRunning = False

    def importMapping(self, modulePath):
        '''
        Method for importing python modules which were specified by an absolute path name.
        @param modulePath: The path to the module.
        @return: The imported module.
        '''
        (moduleDir, moduleFile) = os.path.split(modulePath)
        (moduleName, moduleExt) = os.path.splitext(moduleFile)
        if moduleExt != ".py":
            logging.error(
                "Module isn't a python file. Extension is '%s'" % moduleExt)
            return None
        sys.path.append(moduleDir)
        module = None
        try:
            retVal = __import__(moduleName, globals(), locals(), [], 0)
        except Exception as e:
            logging.error("Trouble importing mappings: " + e.__str__())
            return None
        return retVal


def startService(serviceType=HSN2Service, taskProcessor=None):
    '''
    Main function for starting a Python HSN 2 Service.
    @param serviceType: The service class.
    @param taskProcessor: The taskProcessor class.
    '''
    service = serviceType(taskProcessor)
    cliargs = service.cliparse()
    from hsn2_commons import loggingSetup
    loggingSetup.setupLogging(logPath="/var/log/hsn2/%s.log" %
                              cliargs.serviceName, logToStream=True, logLevel=cliargs.logLevel)
    signal.signal(signal.SIGINT, service.signalHandler)
    signal.signal(signal.SIGTERM, service.signalHandler)
    if service.sanityChecks(cliargs) is False:
        logging.error("Service not starting!")
        exit(-1)
    try:
        service.start(cliargs)
        service.run()
        service.stop()
    except Exception as exc:
        logging.exception(exc)
        exit(-1)

if __name__ == '__main__':
    from hsn2_commons.hsn2taskprocessor import HSN2TaskProcessor as Processor
    startService(taskProcessor=Processor)
