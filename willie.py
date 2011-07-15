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

"""Utility to slowly delete data from a table."""

__author__ = 'darrinw@google.com (Darrin Ward)'

import time

from pylib import app
from pylib import db
from pylib import flags
from willie_lib import willie

FLAGS = flags.FLAGS

flags.DEFINE_boolean('dry_run', True,
                     'Don\'t actually make any changes to the database.')
flags.DEFINE_integer('limit', 100, 'Limit for select query')
flags.DEFINE_integer('utilization_percent', 1, 'Utilization limit')
flags.DEFINE_string('db', None, 'DBSpec to run on')
flags.DEFINE_string('table', None, 'Table to operate on')
flags.DEFINE_string('condition', None, 'Column condition'
                    'to select rows to delete')


def main(args):
  assert FLAGS.db, 'Please pass --db'
  assert FLAGS.table, 'Please pass --table'
  assert FLAGS.condition, 'Please pass --condition'

  dbh = db.Connect(FLAGS.db)

  groundskeeper = willie.Willie(dbh, FLAGS.table, FLAGS.condition,
                                FLAGS.limit, FLAGS.utilization_percent,
                                FLAGS.dry_run)
  groundskeeper.Loop()

  dbh.Close()


if __name__ == '__main__':
  app.run()
