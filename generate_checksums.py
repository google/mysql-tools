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


"""Generate checksums on a database, for online data drift checking.

More information is available here:
http://code.google.com/p/google-mysql-tools/wiki/OnlineDataDrift

Original author: Ben Handy
Later maintainer: Mikey Dickerson
"""

import logging
import time

from drift_lib import drift_lib
from pylib import app
from pylib import db
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('db', None,
                    'DB spec to a primary and the database to checksum')
flags.DEFINE_integer('hours_to_run', None,
                     'Total time allotted to compute all checksums')
flags.DEFINE_float('utilization', 0.02,
                   'Fraction of time to query db (overrides hours_to_run)')
flags.DEFINE_integer('rows_per_query', None,
                     'Suggested size of a single checksum query (rows)')
flags.DEFINE_float('secs_per_query', 1.0,
                   'Duration (seconds) per query (overrides rows_per_query)')
flags.DEFINE_integer('scan_rate', 10000000,
                     'Estimated checksum speed in input-bytes per second')
flags.DEFINE_string('column_types_to_skip', 'blob,longblob',
                    'Comma separated list of datatypes to skip checksumming')
flags.DEFINE_multistring('skip_table', [],
                         'Table to skip checksumming')
flags.DEFINE_multistring('skip_db', ['information_schema', 'adminlocal'],
                         'Database to skip checksumming')
flags.DEFINE_multistring('check_table', [],
                         'Comma separated list of tables to checksum')
flags.DEFINE_multistring('check_engine', ['InnoDB'],
                         'Storage engines to checksum')
flags.DEFINE_string('result_table', 'admin.Checksums',
                    'Name of the table containing the resulting checksums')
flags.DEFINE_string('golden_table', 'admin.ChecksumsGolden',
                    'Name of the db.table for correct checksums results')
flags.DEFINE_string('log_table', 'admin.ChecksumLog',
                    'Name of the db.table where we log completed runs')
flags.DEFINE_string('job_started', None,
                    'Checksum job started timestamp (defaults to now)')
flags.DEFINE_string('row_condition', '',
                    'SQL condition added to checksum query for all tables')


def main(unused_argv):
  """This drives the standalone checksumming app.

  This function sets up the parameters for a DbChecksummer, passing the FLAGS
  parameters in as the droid-compatible config dictionary.
  """
  assert FLAGS.db, 'Please pass --db'

  dbh = db.Connect(FLAGS.db)
  db_checksummer = drift_lib.DbChecksummer(dbh=dbh,
                                           result_table=FLAGS.result_table,
                                           golden_table=FLAGS.golden_table,
                                           log_table=FLAGS.log_table,
                                           job_started=FLAGS.job_started,
                                           scan_rate=FLAGS.scan_rate,
                                           secs_per_query=FLAGS.secs_per_query,
                                           rows_per_query=FLAGS.rows_per_query,
                                           hours_to_run=FLAGS.hours_to_run,
                                           utilization=FLAGS.utilization,
                                           tables_to_skip=FLAGS.skip_table,
                                           databases_to_skip=FLAGS.skip_db,
                                           engines_to_check=FLAGS.check_engine,
                                           tables_to_check=FLAGS.check_table)
  db_checksummer.ChecksumTables()
  dbh.Close()

if __name__ == '__main__':
  app.run()
