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
This module serves as a wrapper for object attributes between HSN2 and protocol buffers.
'''

# used for storing name by value
NameToNumber = dict()
NumberToName = dict()


def getName(obj, listName, val):
    '''
    Retrieve the name of the enum by it's value.
    @param obj: The object for which the enum value is defined.
    @param listName: The enumeration list in which the value appears.
    @param val: The value for which to retrieve the name.
    @return: String name of the enumeration item.
    '''
    loadList(obj, listName)
    return NumberToName.get(listName).get(val)


def getValue(obj, listName, key):
    '''
    Retrieve the value of the enum by it's name.
    @param obj: The object for which the enum value is defined.
    @param listName: The enumeration list in which the value appears.
    @param key: The name for which to retrieve the value.
    @return: integer value of the enumeration item.
    '''
    loadList(obj, listName)
    return NameToNumber.get(listName).get(key)


def loadList(obj, listName):
    '''
    Load the enumeration list into memory.
    @param obj: The object for which the enum value is defined.
    @param listName: The enumeration list in which the value appears.
    '''
    if NameToNumber.get(listName) is None:
        NameToNumber[listName] = dict(
            [(item.name, item.number) for item in obj.DESCRIPTOR.enum_types_by_name[listName].values])
    if NumberToName.get(listName) is None:
        NumberToName[listName] = dict(
            zip(NameToNumber[listName].values(), NameToNumber[listName].keys()))
