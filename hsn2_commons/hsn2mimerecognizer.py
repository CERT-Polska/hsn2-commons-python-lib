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
import re
import subprocess

from hsn2_commons.hsn2osadapter import ObjectStoreException
from hsn2_commons.hsn2service import HSN2Service
from hsn2_commons.hsn2service import startService
from hsn2_commons.hsn2taskprocessor import HSN2TaskProcessor
from hsn2_commons.hsn2taskprocessor import ParamException, ProcessingException


class MimeTypeDetectorException(Exception):
    pass


class HSN2MimeRecognizer(HSN2TaskProcessor):
    '''
    TaskProcessor used for recognizing file mime-types.
    '''
    mapping = {
        "text/plain": "text",
        "application/x-executable": "binary",
        "application/x-shockwave-flash": "swf",
        "application/pdf": "pdf",
        "application/msword": "doc",
        "application/x-gzip": "gz",
        "application/x-tar": "tar"
    }

    def taskProcess(self):
        '''
        Responsible for calling the recognize method on the current task.
        @return: List of warnings collected while processing.
        '''
        logging.debug(self.__class__)
        logging.debug(self.currentTask)
        logging.debug(self.objects)
        warnings = []
        if len(self.objects) == 0:
            raise ObjectStoreException(
                "Task processing didn't find task object.")
        if self.objects[0].isSet("content"):
            filepath = self.dsAdapter.saveTmp(
                self.currentTask.job, self.objects[0].content.getKey())
        else:
            raise ParamException("Content attribute is not set.")
        mtype = self.recognize(filepath)
        mtype = self.mapping.get(mtype, "undefined")
        self.objects[0].addString("type", mtype)
        self.dsAdapter.removeTmp(filepath)
        self.osAdapter.objectsUpdate(
            self.currentTask.job, self.objects, overwrite=True)
        return warnings

    def recognize(self, filename):
        '''
        Calls the file command with appropriate options in order to recognize the files mimetype.
        @param filename: The path to the file which is to be identified.
        @return: The identified mime-type.
        '''
        proc = subprocess.Popen(["file", "--mime-type", filename],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        result = proc.communicate()
        m = re.search('ERROR', result[0])
        if m is not None or proc.returncode != 0:
            raise ProcessingException("Mime recognizer failed")
        return result[0].split(":")[1].strip()

if __name__ == '__main__':
    HSN2Service.serviceName = "mime-recognizer"
    startService(HSN2Service, HSN2MimeRecognizer)
