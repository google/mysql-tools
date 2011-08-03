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

__author__ = 'flamingcow@google.com (Ian Gulliver)'


from pylib import app
from pylib import db
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_string('db', None, 'DB spec to scan')


def _ListStartsWith(superset, subset):
  """Returns true if superset starts with subset."""
  return superset[:len(subset)] == subset


def FindDuplicateIndexes(dbh):
  tables = dbh.ExecuteOrDie(
      'SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES')

  for table in tables:
    indexes = dbh.ExecuteOrDie('SHOW INDEX FROM `%s`.`%s`' %
                               (table['TABLE_SCHEMA'], table['TABLE_NAME']))
    columns = {}
    for index in indexes:
      if index['Non_unique'] == 0:
        # Skip unique indexes
        continue
      columns.setdefault(index['Key_name'], []).append(index['Column_name'])

    for key_name1, column_list1 in columns.iteritems():
      for key_name2, column_list2 in columns.iteritems():
        if key_name1 == key_name2:
          continue
        if _ListStartsWith(column_list1, column_list2):
          print '`%s`.`%s`: Key %s is a prefix of %s' % (
              table['TABLE_SCHEMA'], table['TABLE_NAME'],
              key_name2, key_name1)


def main(argv):
  assert FLAGS.db, 'Please pass --db'

  with db.Connect(FLAGS.db) as dbh:
    FindDuplicateIndexes(dbh)


if __name__ == '__main__':
  app.run()
