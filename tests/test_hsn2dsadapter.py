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
sys.path.append("/opt/hsn2/python/commlib")
from hsn2dsadapter import HSN2DataStoreAdapter, DataStoreException
import unittest
import os

class testHSN2DataStoreAdapter(unittest.TestCase):
	ds = None

	@classmethod
	def setUp(self):
		self.ds = HSN2DataStoreAdapter("localhost:8080")

	def testPutPass(self):
		ident = self.ds.putFile("/etc/hosts", 139)
		self.assertRegexpMatches(ident, "\d+", "Returned id isn't an integer")
		return ident

	def testPutFailDirectory(self):
		self.assertRaises(IOError, self.ds.putFile, "/etc/", 139)

	def testPutFailNonExistent(self):
		self.assertRaises(IOError, self.ds.putFile, "/tmp/aaa", 139)

	def testGetPass(self):
		ident = self.testPutPass()
		content = self.ds.getFile(139, ident)
		data = open("/etc/hosts", 'rb').read()
		self.assertEqual(content, data, "Retrieved content is different from submitted")

	def testGetFail(self):
		self.assertRaises(DataStoreException, self.ds.getFile, 139, "aaa")

	def testSavePass(self):
		ident = self.testPutPass()
		self.assertFalse(os.path.isfile("/tmp/aaa"))
		self.ds.saveFile(139, ident, "/tmp/aaa")
		self.assertTrue(os.path.isfile("/tmp/aaa"))
		self.ds.removeTmp("/tmp/aaa")
		self.assertFalse(os.path.isfile("/tmp/aaa"))

	def testSaveFail(self):
		ident = self.testPutPass()
		self.assertRaises(IOError, self.ds.saveFile, 139, ident, "/tmp/")

	def testTmpPass(self):
		ident = self.testPutPass()
		fpath = self.ds.saveTmp(139, ident)
		self.assertTrue(os.path.isfile(fpath))
		self.ds.removeTmp(fpath)
		self.assertFalse(os.path.isfile(fpath))
