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

Example dbspec:
  localhost:root::test

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

from pylib import db


_CSV_RE = re.compile('^\s*CSV\s+(?P<query>.*)$', re.IGNORECASE | re.DOTALL)


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
    by_result.setdefault(str(result), []).append(name)
  if len(by_result) > 1:
    for result, names in by_result.iteritems():
      names.sort()
      print '%s:\n%s' % (names, result)
  else:
    if result:
      print result


def main(argv):
  dbh = db.Connect(argv[1])

  try:
    histfile = os.path.join(os.environ['HOME'], '.mysql_history')
    readline.read_history_file(histfile)
    atexit.register(readline.write_history_file, histfile)
  except (KeyError, IOError):
    pass

  if sys.stdin.isatty():
    baseprompt = '%s> ' % argv[1]
    continueprompt = '%s> ' % (' ' * len(argv[1]))
  else:
    baseprompt = ''
    continueprompt = ''

  while True:
    try:
      line = ''
      line = raw_input(baseprompt)
      if not line:
        continue
      if line.strip()[0:2] != '--':
        while line.strip()[-1] != ';':
          line = line + '\n' + raw_input(continueprompt)
      Execute(dbh, line)
    except EOFError:
      if line:
        Execute(dbh, line)
      else:
        print
      dbh.Close()
      return 0
    except KeyboardInterrupt:
      dbh.Close()
      return 0

if __name__ == '__main__':
  main(sys.argv)
