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

"""A simple script to connect to a MySQL DB and update Heartbeat.

This simple script connects to a database and begins an infinite loop
executing a query to update the Heartbeat table.  It will update the
table every --interval seconds.  Every --connection_lifetime seconds,
replication_heartbeat will reconnect.  This script will suicide if
a valid heartbeat doesn't happen within 2*connection_lifetime seconds.

Usage:
  replication_heartbeat.py --dbspec="host:user:port:db" <--interval=n>

Example dbspecs:
  localhost:root::test
  dbhost:root:pfile=.passwordfile:dbname:12345  # port number
  socket=/var/lib/mysql.sock:root:pfile=/dev/null:dbname
"""

__author__ = 'kacirek@google.com (Mike Kacirek)'
# Original Author:  chip@google.com (Chip Turner)

import logging
import signal
import time

from pylib import app
from pylib import db
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_integer('connection_lifetime', 60, 'Time (s) between reconnects '
                     'to the database')
flags.DEFINE_string('dbspec', None, 'DBSpec of database to connect to')
flags.DEFINE_float('interval', 1.0, 'Interval in (s) of heartbeat pulse.')


def BeatOnce(dbh, interval):
  sql = """
    UPDATE Heartbeat
    SET LastHeartbeat = UNIX_TIMESTAMP()
     WHERE LastHeartbeat <= UNIX_TIMESTAMP() - %f
    """ % (interval - 1.0)
  dbh.ExecuteOrDie(sql)


def DoHeartbeat(dbh, interval, connection_lifetime):
  """Update the Heartbeat table.

    Update the Heartbeat table once per <interval> for
    <connection_lifetime> seconds.

  Args:
    dbh: dbhandle to run heartbeat on.
    interval: time in (s).  Pulse frequency.
    connection_lifetime: time in (s).  Time between reconnects.
  """

  start_time = time.time()
  while time.time() < start_time + connection_lifetime:
    BeatOnce(dbh, interval)
    time.sleep(interval)


def main(argv):

  if not FLAGS.dbspec:
    raise app.UsageError('You must specify a dbspec.')

  dbspec = db.Spec.Parse(FLAGS.dbspec)
  if not dbspec.IsSingle():
    logging.fatal('Must specify a single database shard to operate on.')

  logging.info('Heartbeat starting up...')
  while True:
    # Set a signal so that, no matter what, we guarantee we won't just
    # get stuck.  If we don't make it back to the top of this loop by
    # the time the alarm expires, our process will terminate.
    signal.alarm(FLAGS.connection_lifetime * 2)
    with dbspec.Connect() as dbh:
      # This is likely the default, but if its not, each beat should be a
      # transaction.
      dbh.ExecuteOrDie('SET AUTOCOMMIT=1;')
      DoHeartbeat(dbh, FLAGS.interval, FLAGS.connection_lifetime)
    logging.info("Feelin' fine, reconnecting...")


if __name__ == '__main__':
  app.run()
