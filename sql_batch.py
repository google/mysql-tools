# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Batch MySQL client that supports sharded databases.

Usage:
  sql_batch.py --query 'select 1' --output_type csv <dbspec>
  echo 'select 1;'|sql_batch --output_type csv <dbspec>

Example dbspecs:
  localhost:root::test
  dbhost:root:pfile=.passwordfile:dbname:12345  # port number
  socket=/var/lib/mysql.sock:root:pfile=/dev/null:dbname
"""

__author__ = 'lasersox@google.com (AJ Ross)'

import csv
import sys

import gflags

from pylib import app
from pylib import db

FLAGS = gflags.FLAGS

gflags.DEFINE_string('charset', 'utf8',
                     'Input/output character set')
gflags.DEFINE_string('query', None,
                     'An SQL query.')
gflags.DEFINE_string('output_type', 'csv',
                     'The output type, eg, csv.')
gflags.DEFINE_boolean('header', True,
                      'Add header to CSV.')


class Error(Exception):
  pass


class CSVResultWriter(object):
  """Writes query results to a CSV file."""

  def __init__(self):
    self._fh = None

  def __enter__(self):
    self._csv_writer = csv.writer(sys.stdout)
    return self

  def __exit__(self, unused_type, unused_value, unused_traceback):
    if self._fh:
      self._csv_writer = None

  def Write(self, result):
    if FLAGS.header:
      self._csv_writer.writerow(result.GetFields())
    for row in result.GetRows():
      fields = []
      for field in row:
        if isinstance(field, unicode):
          fields.append(field.encode(FLAGS.charset))
        else:
          fields.append(field)
      self._csv_writer.writerow(fields)


_RESULT_WRITERS = {'csv': CSVResultWriter}


def main(argv):
  if len(argv) != 2:
    raise app.UsageError('Please specify a dbspec')

  if FLAGS.output_type not in _RESULT_WRITERS:
    raise app.UsageError('Unrecognized output type %r. Must be one of %r.' % (
        FLAGS.output_type, _RESULT_WRITERS.keys()))
  result_writer_cls = _RESULT_WRITERS[FLAGS.output_type]

  with db.Connect(argv[1], charset=FLAGS.charset) as dbh:
    results = dbh.ExecuteOrDie(FLAGS.query or sys.stdin.read())
    with result_writer_cls() as result_writer:
      result_writer.Write(results)
