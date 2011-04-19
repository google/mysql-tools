#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



"""Creates a mysql connection that functions like the droid connection module.

The purpose is to a allow a standalone datadrift application to share code with
the droid module datadrift application. Some of the code has been copied from
ads/db/droid/modules/mysql_connection.py.

More information is available here:
http://wiki.corp.google.com/Main/MysqlDriftChecker

Original author: Ben Handy
Later maintainer: Mikey Dickerson
"""

import getpass
import logging
import os
import time
import MySQLdb


def PromptForPassword(user):
  return getpass.getpass('Enter mysql password for %s: ' % user)


class QueryResult(object):
  """A class to hold a SQL query result.

  QueryResult objects store the result internally as a list of field names and
  rows as tuples, but pretend to the world to be full-fledged dictionary
  cursors.  This is just a memory-saving hack.

  This class was copied directly from droid's mysql_connection.py
  """

  def __init__(self, fields, result):
    self._fields = fields
    self._result = result

  def __getitem__(self, i):
    return dict(zip(self._fields, self._result[i]))

  def __len__(self):
    return len(self._result)


class ImitateDroidConnection(object):
  """Connects to a mysql server and exposes a Query function like droid's."""

  def __init__(self, db_host, db_user, db_pass_file, db_name, db_port=3306,
               db_socket=None):
    self._db_host = db_host
    self._db_user = db_user
    self._db_name = db_name
    self._db_socket = db_socket
    self._db_port = db_port
    self._cursor = None
    if db_pass_file:
      file_handle = open(os.path.expanduser(db_pass_file))
      self._db_password = file_handle.read().strip()
    else:
      self._db_password = PromptForPassword(db_user)

    self._db = None

  def Connect(self, retry=10):
    """Connect to the MySQL server.

    Attempt to connect to MySQL.  Save a cursor for use by Query().

    Args:
      retry: Number of times to try reconnecting before finally giving up.
    """
    logging.info('Connecting to MySQL')
    try:
      if self._db_socket is not None:
        self._db = MySQLdb.connect(unix_socket=self._db_socket,
                                   user=self._db_user,
                                   passwd=self._db_password,
                                   db=self._db_name)
      else:
        self._db = MySQLdb.connect(host=self._db_host,
                                   port=self._db_port,
                                   user=self._db_user,
                                   passwd=self._db_password,
                                   db=self._db_name)
      self._cursor = self._db.cursor(MySQLdb.cursors.Cursor)
      logging.info('Connected to MySQL')
    except MySQLdb.Error, e:
      if not retry:
        logging.error('Failed to connect to MySQL: %s.' % e)
        raise
      logging.error('Failed to connect to MySQL: %s.  Will retry.' % e)
      time.sleep(10)  # This is arbitrary, some kind of backoff might be nice.
      self.Connect(retry-1)
      return

  def Query(self, query):
    """Execute a query on the MySQL server.

    Args:
      query: The SQL query to execute
    Returns:
      A list of dictionaries for each row in the result, or None if the query
      failed.
    """
    while True:
      try:
        self._cursor.execute(query)
        result = self._cursor.fetchall()
        if not result:
          return result
        fields = [i[0] for i in self._cursor.description]
        return QueryResult(fields, result)
      # MySQLdb has a race condition where it raises TypeError sometimes when
      # the SQL server goes away
      except (MySQLdb.Error, TypeError), e:
        if e[0] in (2002, 2006):
          logging.info('disconnected, reconnecting')
          logging.error('Disconnected from MySQL (%s); reconnecting.' % e)
          self._cursor.close()
          self._db.close()
          self.Connect()
        else:
          logging.error('SQL query returned an error: %s.' % e)
          return None
