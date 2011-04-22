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

from drift import drift_lib
from drift import imitate_droid_connection
from pylib import app
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('databases_to_check', None,
                    'comma separated list of database to checksum')
flags.DEFINE_string('db_host', None,
                    'Hostname to generate checksums on (should be master)')
flags.DEFINE_string('db_user', '',
                    'Username to generate checksums with (must have super)')
flags.DEFINE_string('db_pass_file', '',
                    'Path to password file for db_user account')
flags.DEFINE_integer('hours_to_run', 0,
                     'Total time allotted to compute all checksums')
flags.DEFINE_float('utilization', 0.02,
                   'Fraction of time to query db (overrides hours_to_run)')
flags.DEFINE_integer('rows_per_query', 0,
                     'Suggested size of a single checksum query (rows)')
flags.DEFINE_float('secs_per_query', 1.0,
                   'Duration (seconds) per query (overrides rows_per_query)')
flags.DEFINE_integer('scan_rate', 10000000,
                     'Estimated checksum speed in input-bytes per second')
flags.DEFINE_string('column_types_to_skip', 'blob,longblob',
                    'Comma separated list of datatypes to skip checksumming')
flags.DEFINE_string('tables_to_skip', '',
                    'Comma separated list of tables to skip checksumming')
flags.DEFINE_string('tables_to_check', '',
                    'Comma separated list of tables to checksum')
flags.DEFINE_string('engines_to_check', 'InnoDB',
                    'Comma separated list of storage engines to checksum')
flags.DEFINE_string('result_table', 'test.Checksums',
                    'Name of the table containing the resulting checksums')
flags.DEFINE_string('golden_table', 'test.ChecksumsGolden',
                    'Name of the db.table for correct checksums results')
flags.DEFINE_string('job_started', None,
                    'Checksum job started timestamp (defaults to now)')
flags.DEFINE_string('row_condition', '',
                    'SQL condition added to checksum query for all tables')


def DictFromFlags():
  """Return a normal dictionary out of the FLAGS dictionary.

  Returns:
    Dictionary mapping flag keys to flag values.
  """
  flag_dict = vars(FLAGS)
  new_dict = {}
  for k, v in flag_dict.iteritems():
    new_dict[k] = v
  return new_dict


def main(unused_argv):
  """This drives the standalone checksumming app.

  This function sets up the parameters for a DbChecksummer, passing the FLAGS
  parameters in as the droid-compatible config dictionary.
  """

  if not FLAGS.db_host:
    logging.error('must provide --db_host (master) to run checksum queries on')

  dbs = FLAGS.databases_to_check.split(',')
  for db in dbs:
    # Create DB connection
    connection = imitate_droid_connection.ImitateDroidConnection(
        FLAGS.db_host, FLAGS.db_user, FLAGS.db_pass_file, db)
    connection.Connect()
    db_checksummer = drift_lib.DbChecksummer(connection.Query,
                                             DictFromFlags(), db)
    db_checksummer.PrepareToChecksum()
    while db_checksummer.ChecksumQuery():
      time.sleep(db_checksummer.GetNextWait())
      logging.info(db_checksummer.ReportPerformance())

if __name__ == '__main__':
  app.run()
