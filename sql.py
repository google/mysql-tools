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

"""Commandline MySQL client that supports sharded databases.

Usage:
  sql.py <dbspec>

Example dbspecs:
  localhost:root::test
  dbhost:root:pfile=.passwordfile:dbname:12345  # port number
  socket=/var/lib/mysql.sock:root:pfile=/dev/null:dbname

This extends SQL syntax by adding support for:
  -- Output as comma-separated values.
  CSV SELECT * FROM foo;
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import atexit
import csv
import os
import re
import readline
import sys

from pylib import app
from pylib import db
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('charset', 'utf8', 'Input/output character set')
flags.DEFINE_string('prompt', None, 'Custom prompt instead of the dbspec')
flags.DEFINE_boolean('print_field_names', True,
                     'Print field names to CSV and stdout.')
_CSV_RE = re.compile('^\s*CSV\s+(?P<query>.*)$',
                     re.IGNORECASE | re.DOTALL | re.MULTILINE)


def _Encode(value):
  if isinstance(value, unicode):
    return value.encode(FLAGS.charset)
  else:
    return value


def _ShowTable(result):
  if result is None:
    return
  if result:
    for row in result.GetTable(FLAGS.print_field_names):
      print _Encode(row)
  rowcount = result.GetRowsAffected()
  # Heuristic for whether this is useful information.
  if rowcount is not None and rowcount != len(result):
    print '%d row(s) affected' % rowcount


class CancellableExecutor(object):
  """This class helps executing a single query and cancelling it.

  Properties:
    dbh: A database connection as per db.Connect().
    ongoing_operation: A db.Operation representing the latest Execute() call.
  """

  def __init__(self, dbh):
    """Builds a new instance.

    Args:
      dbh: A database connection as per db.Connect().
    """
    self.dbh = dbh
    self.ongoing_operation = None

  def CancelOngoingOperation(self):
    if self.ongoing_operation:
      self.dbh.Cancel(self.ongoing_operation)
      self.ongoing_operation = None

  def Execute(self, query):
    """Executes the query and displays the results on screen."""
    csvh = None
    csv_match = _CSV_RE.match(query)
    if csv_match:
      csvh = csv.writer(sys.stdout)
      query = csv_match.group('query')

    self.ongoing_operation = self.dbh.Submit(query)
    results = self.dbh.Wait(self.ongoing_operation)
    for key, value in results.items():
      results[key] = list(value)[0]
    self.ongoing_operation = None

    if csvh:
      if FLAGS.print_field_names:
        csvh.writerow(results.values()[0].GetFields())
      for host, result in results.iteritems():
        for row in result.GetRows():
          fields = []
          for field in row:
            if isinstance(field, unicode):
              fields.append(field.encode(FLAGS.charset))
            else:
              fields.append(field)
          csvh.writerow(fields)
      return
    by_result = {}
    result = None
    for name, result in results.iteritems():
      by_result.setdefault(result, []).append(name)
    if len(by_result) > 1:
      for result, names in by_result.iteritems():
        if not result:
          continue
        names.sort()
        print '%s:' % names
        _ShowTable(result)
    else:
      _ShowTable(result)


def Execute(dbh, query):
  executor = CancellableExecutor(dbh)
  executor.Execute(query)


def main(argv):
  if len(argv) < 2:
    raise app.UsageError('Please specify a dbspec')

  try:
    histfile = os.path.join(os.environ['HOME'], '.mysql_history')
    readline.read_history_file(histfile)
    atexit.register(readline.write_history_file, histfile)
  except (KeyError, IOError):
    pass

  if sys.stdin.isatty():
    if FLAGS.prompt:
      prompt = '%s> ' % FLAGS.prompt
    else:
      prompt = '%s> ' % argv[1]
    continued_prompt = ' ' * (len(prompt) - 3) + '-> '
  else:
    prompt = continued_prompt = ''

  continued = [False]

  def GetLines():
    while True:
      try:
        display_prompt = continued_prompt if continued[0] else prompt
        continued[0] = True
        yield raw_input(display_prompt).decode(FLAGS.charset)
      except EOFError:
        print
        return

  with db.Connect(argv[1], charset=FLAGS.charset) as dbh:
    for statement in db.XCombineSQL(GetLines()):
      continued[0] = False
      executor = CancellableExecutor(dbh)
      try:
        executor.Execute(statement)
      except KeyboardInterrupt:
        print ('Cancelling current query. '
               'Hitting ^C again will terminate the program.')
        executor.CancelOngoingOperation()


if __name__ == '__main__':
  app.run()
