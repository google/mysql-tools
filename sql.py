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

flags.DEFINE_string('charset', 'utf-8', 'Input/output character set')


_CSV_RE = re.compile('^\s*CSV\s+(?P<query>.*)$',
                     re.IGNORECASE | re.DOTALL | re.MULTILINE)


def Execute(dbh, query):
  csvh = None
  csv_match = _CSV_RE.match(query)
  if csv_match:
    csvh = csv.writer(sys.stdout)
    query = csv_match.group('query')
  results = dbh.MultiExecute(query)
  if csvh:
    csvh.writerow(results.values()[0].GetFields())
    for host, result in results.iteritems():
      csvh.writerows(result.GetRows())
    return
  by_result = {}
  for name, result in results.iteritems():
    by_result.setdefault(result, []).append(name)
  if len(by_result) > 1:
    for result, names in by_result.iteritems():
      names.sort()
      print '%s:\n%s' % (names, unicode(result).encode(FLAGS.charset))
  else:
    if result:
      print unicode(result).encode(FLAGS.charset)


def GetLines(prompt):
  while True:
    try:
      yield raw_input(prompt).decode(FLAGS.charset)
    except EOFError:
      print
      return


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
    prompt = '%s> ' % argv[1]
  else:
    prompt = ''

  with db.Connect(argv[1]) as dbh:
    for statement in db.XCombineSQL(GetLines(prompt)):
      Execute(dbh, statement)


if __name__ == '__main__':
  app.run()
