#!/usr/bin/python2.4
#
# Copyright (C) 2006 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""One-line documentation for dbspec module.

A detailed description of dbspec.
"""

__author__ = 'chip@google.com (Chip Turner)'

import MySQLdb
import subprocess

from gmt import compat_logging as logging
from gmt import compat_flags as flags

FLAGS = flags.FLAGS
flags.DEFINE_string("stop_mysql_command", "sudo /etc/init.d/mysql stop",
                    "command to run on remote host to stop mysql")
flags.DEFINE_string("start_mysql_command", "sudo /etc/init.d/mysql start",
                    "command to run on remote host to start mysql")

class TableInfo(object):
  def __init__(self, name, table_type):
    self.name = name
    self.table_type = table_type

class ColumnInfo:
  """
  Use by DbSpec's getColumns method to return the list of columns in
  a given table.
  """
  def __init__(self, name, field_type):
    self.name = name
    self.field_type = field_type

class DatabaseSpec(object):
  def __init__(self, host, user, password):
    self.user = user
    self.password = password
    self.host = host
    self._connection = None

  def execute(self, query, failOnError=False):
    if not self._connection:
      self.connection = MySQLdb.connect(host=self.host, user=self.user,
                                        passwd=self.password,
                                        connect_timeout=30)

    cursor = self.connection.cursor()
    try:
      cursor.execute(query)
      return cursor.fetchall()
    except MySQLdb.MySQLError, e:
      if failOnError:
        raise e
      return

  def disconnected(self):
    self.connection.close()

  def startMySql(self):
    p = subprocess.Popen(FLAGS.start_mysql_command.split(),
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
      logging.fatal("Failure to start mysql, returned %d\n%s\n%s" %
                    (p.returncode, stdout, stderr))
  def stopMySql(self):
    p = subprocess.Popen(FLAGS.stop_mysql_command.split(),
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
      logging.fatal("Failure to stop mysql, returned %d\n%s\n%s" %
                    (p.returncode, stdout, stderr))

  def getDatabases(self):
    rows = self.execute("SHOW DATABASES")
    return [ r[0] for r in rows ]

  def getTables(self, db):
    rows = self.execute("SHOW TABLE STATUS FROM %s" % db)
    return [ TableInfo(r[0], r[1]) for r in rows ]

  def getColumns(self, table_name):
    columns = self.execute("DESCRIBE %s" % table_name)

    ret = []
    for column in columns:
      column_info = ColumnInfo(column[0], column[1])
      ret.append(column_info)

    return ret
