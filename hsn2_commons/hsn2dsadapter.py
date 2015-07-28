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

import httplib
import os
import re
import tempfile


class DataStoreException(Exception):
    pass


class HSN2DataStoreAdapter():

    def __init__(self, address):
        '''
        @param address: URL to the Data Store.
        '''
        prefix = "http://"
        if address.startswith(prefix):
            address =  address[len(prefix):]
        self.address = address

    def putBytes(self, bytes_, job_id):
        '''
        Uploads a file to the HSN2 Data Store.
        @param filepath: The file to be uploaded.
        @param job_id: The id of the job in which the file is being uploaded.
        @return: The key under which the file is stored.
        '''

        http = httplib.HTTP(self.address)
        try:
            # write header
            http.putrequest("POST", "/data/" + str(job_id))
            http.putheader("User-Agent", "python service")
            http.putheader("Content-Length", str(len(bytes_)))
            http.endheaders()

            # write body
            http.send(bytes_)

            # get response
            errcode, errmsg, headers = http.getreply()

            if errcode != 201:
                raise DataStoreException(
                    "%d - %s - %s" % (errcode, errmsg, headers))
        except Exception as e:
            raise DataStoreException(str(e))

        result = http.getfile().read()

        m = re.search('([0-9]+)', result)
        return m.group(0)
        '''return new event id'''

    def putFile(self, filepath, job_id):
        data = open(filepath, 'rb').read()
        return self.putBytes(data, job_id)

    def getFile(self, job_id, event_id):
        '''
        Downloads a file from the HSN2 Data Store and returns it's contents.
        @param job_id: The id of the job in which the file was uploaded.
        @param event_id: The key under which the file is stored.
        @return: The contents of the file.
        '''
        http = httplib.HTTP(self.address)
        try:
            http.putrequest(
                "GET", "/data/" + str(job_id) + "/" + str(event_id))
            http.putheader("User-Agent", "python service")
            http.endheaders()

            http.send("")
            errcode, errmsg, headers = http.getreply()

            if errcode != 200:
                raise DataStoreException(
                    "U" + "%d - %s - %s" % (errcode, errmsg, headers))
        except Exception as e:
            raise DataStoreException(str(e))
            #raise DataStoreException(http.getfile().read())

        return http.getfile().read()

    def saveFile(self, job_id, event_id, filepath):
        '''
        Downloads a file from the HSN2 Data Store and saves it in the specified path.
        @param job_id: The id of the job in which the file was uploaded.
        @param event_id: The key under which the file is stored.
        @param filepath: The path where the file is to be saved. The name of the file should already be part of the path.
        '''
        fileContent = self.getFile(job_id, event_id)
        f = open(filepath, "wb")
        f.write(fileContent)
        f.close()

    def saveTmp(self, job_id, event_id, prefix='tmp', directory=None):
        '''
        Downloads a file from the HSN2 Data Store and saves it in a temporary file.
        Caution. File needs to be removed manually.
        @param job_id: The id of the job in which the file was uploaded.
        @param event_id: The key under which the file is stored.
        @param prefix: Prefix to use for the filename. Defaults to "tmp". The file will be suffixed with ".tmp".
        @param directory: Directory in which the file is to be stored. Defaults to "/tmp".
        @return: The path to the file.
        '''
        data = self.getFile(job_id, event_id)
        (fhandle, fpath) = tempfile.mkstemp(
            ".tmp", prefix=prefix, dir=directory, text=False)
        os.write(fhandle, data)
        os.close(fhandle)
        return fpath

    def removeTmp(self, filepath):
        '''
        Deletes a file. Prepared for use with saveTmp.
        @param filepath: The path to the file.
        '''
        if os.path.exists(filepath):
            os.remove(filepath)
