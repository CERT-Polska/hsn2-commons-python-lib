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

import logging
import os


def setupLogging(logPath=None, logToStream=True, logLevel=logging.DEBUG):
    '''
    Setup up logging
    @param logPath: The file to which log messages are to be appended. If None then no log file will be kept.
    @param logToStream: Whether log messages are to be printed to stderr.
    @param logLevel: The required level for a message to be logged.
    '''
    log = logging.getLogger()
    log.handlers = list()  # remove existing handlers
    log.setLevel(_checkLevel(logLevel))
    logFormatter = logging.Formatter("%(asctime)s [%(process)d] %(levelname)s %(message)s")

    if logPath is not None:
        logPath = os.path.abspath(logPath)
        logDir = os.path.dirname(logPath)
        if not os.path.exists(logDir):
            os.makedirs(logDir)
        fileHandler = logging.FileHandler(logPath)
        fileHandler.setFormatter(logFormatter)
        log.addHandler(fileHandler)
        log.info("Logging to file initialised.")

    if logToStream:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(logFormatter)
        log.addHandler(consoleHandler)

# Copied from Python 2.7 source code
# starts here ----->
_levelNames = {
    logging.CRITICAL: 'CRITICAL',
    logging.ERROR: 'ERROR',
    logging.WARNING: 'WARNING',
    logging.INFO: 'INFO',
    logging.DEBUG: 'DEBUG',
    logging.NOTSET: 'NOTSET',
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARN': logging.WARNING,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG,
    'NOTSET': logging.NOTSET,
}


def _checkLevel(level):
    if isinstance(level, int):
        rv = level
    elif str(level) == level:
        if level not in _levelNames:
            raise ValueError("Unknown level: %r" % level)
        rv = _levelNames[level]
    else:
        raise TypeError("Level not an integer or a valid string: %r" % level)
    return rv
# <--------- ends here

if __name__ == '__main__':
    setupLogging("/tmp/test.log")
    logging.debug("Test")
