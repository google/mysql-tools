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

"""Utility to push and publish permissions.

  permissions.py [flags] FILENAME COMMAND [ARG]...
  permissions.py [flags] copy DB_SET

Note the --db=DBSPEC flag is required for copy/publish/push commands and may
be required for push/test depending on your permissions configuration.
Examples:
  --db=dbhost#:::dbname#
  --db=dbhost#:otheruser::dbname#

FILENAME contains permissions settings on which to operate. The file may
contain one or more named sets of permissions.

Available commands:
  copy SET_NAME
    Copy the published permissions SET_NAME to DBSPEC.
    This is the only command which is used without a FILENAME.
  dump SET_NAME
    Dump the SQL for the permissions SET_NAME from FILENAME.
  list-sets
    List the permissions set names defined in FILENAME.
  publish SET_NAME
    Publish the permissions set named by --source_set to DBSPEC as SET_NAME.
  push SET_NAME
    Push permissions set named by SET_NAME directly to DBSPEC.
  test
    Test all permissions sets defined in FILENAME.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import sys

from permissions_lib import define
from permissions_lib import use
from pylib import app
from pylib import flags
from pylib import db

FLAGS = flags.FLAGS

flags.DEFINE_string('db', None, 'DB spec to read from/push to')
flags.DEFINE_integer('push_duration', 1800, 'Staggered push duration (seconds)')
flags.DEFINE_string('source_set', None,
                    'Set name to publish from; defaults to target set')


_COMMANDS = {'dump':      'Dump',
             'list-sets': 'PrintSets',
             'push':      'Push',
             'publish':   'Publish',
             'test':      'Test'}


def main(argv):
  if len(argv) < 3:
    raise app.UsageError('Not enough arguments')

  if FLAGS.db:
    dbh = db.Connect(FLAGS.db)
  else:
    dbh = None

  try:  # defer dbh.Close()
    if argv[1] == 'copy':
      use.Copy(dbh, argv[2])
      return 0

    f = open(argv[1], 'r')
    perms = use.PermissionsFile(f.read())
    command = argv[2]
    args = argv[3:]

    if command == 'publish':
      if not FLAGS.source_set:
        FLAGS.source_set = args[0]
      args = [FLAGS.source_set, args[0], FLAGS.push_duration]

    try:
      getattr(perms, _COMMANDS[command])(dbh, *args)
    except db.Error, e:
      # Lose the stack trace; it's not useful for DB errors
      print e
      return 1
    except TypeError, e:
      print e
      return 2

  finally:
    if dbh is not None:
      dbh.Close()


if __name__ == '__main__':
  app.run()
