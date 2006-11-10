#!/usr/bin/python2.4
#
# Copyright (C) 2006 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""compact_innodb.py [ --num_readers N ] [ --num_writers N ] \\
                         [ --dump_dir /path/to/save/dumps ] host:dbuser:dbpass:

A script to mysqldump and reload the InnoDB tables in a given database.

This script takes a given database and performs a mysqldump on all of
the InnoDB tables present.  It then drops the innodb tables, stops
mysql, deletes the innodb datafile and logs, and then restarts mysql.
It then uses the mysql utility to reload the dumps.

The script can be limited to only doing the dump or only doing the restore.
Among other things, this is used to restore data into MySQL 5 that had
been dumped from MySQL 4.
"""

__author__ = 'chip@google.com (Chip Turner)'

import glob
import MySQLdb
import os
import re
import subprocess
import sys
import time

import command_pool
import config_helper
config_helper.Init() # here instead of __main__ because we need the
                     # defaults for our string defines
import compat_logging as logging
import compat_flags as flags
import dbspec_lib

FLAGS = flags.FLAGS
flags.DEFINE_integer("num_readers", 6, "number of parallel readers (mysqldump)")
flags.DEFINE_integer("num_writers", 4, "number of parallel writers")
flags.DEFINE_string("dump_dir", "/export/hda3/tmp/sql-dumps",
                    "directory to store .sql.gz dumps")
flags.DEFINE_boolean("do_dump", True, "dump the data")
flags.DEFINE_boolean("do_reload", True, "reload the data")
flags.DEFINE_boolean("tolerate_lossy_fp", False,
                     "tolerate loss of precision on reload of FP types")
flags.DEFINE_string("mysql_bin_dir", '', "path to MySQL binaries")
flags.DEFINE_boolean("wipe_innodb", True,
                     "drop tables, restart MySQL")

flags.DEFINE_string("user", config_helper.GetGlobal("user"),
                    "User to connect to the databases as",
                    short_name="u")
flags.DEFINE_string("password", config_helper.GetGlobal("password"),
                    "Password to use when connecting as user",
                    short_name="p")
flags.DEFINE_string("my_cnf_location", "/etc/my.cnf",
                    "where my.cnf is stored")
flags.DEFINE_string("mysql_root", "/var/lib/mysql",
                    "location of mysql's root, aka, where it stores data")

DUMP_RE = re.compile(r'(\w+)-(\w+)\.sql\.gz')

def _CheckReplicationConfigured(dbspec):
  """
  Test whether replication is enabled on this slave.

  Replication may be broken; this checks if it is configured.

  Args:
    dbspec - database to check

  Returns:
    boolean indicating if replication is enabled and running
  """
  rows = dbspec.execute("SHOW SLAVE STATUS")
  if len(rows) == 0:
    return False
  if rows[0][9] == 'No' and rows[0][10] == 'No':
    return False
  return True


def DumpTables(dbspec, num_readers, dump_dir, mysql_bin_dir, lossy_fp):
  dbs = dbspec.getDatabases()
  db_info = {}
  for db in dbs:
    db_info[db] = {}
    for table in dbspec.getTables(db):
      db_info[db][table.name] = table

  todo = []
  for db in db_info:
    for table in db_info[db].values():
      if table.table_type == 'InnoDB':
        todo.append((db, table))

  logging.info("Found %d tables to compact" % len(todo))

  # sort descending, first by size then by name
  todo.sort(lambda a,b: cmp(b[1].size, a[1].size) or cmp(b[1].name, a[1].name))

  pool = command_pool.CommandPool(num_readers)
  for db, table in todo:
    # we use PIPESTATUS to get the return of mysqldump, not gzip.
    # also, --lossless-fp is an extension to our mysqldump that allows
    # for truly lossless dumps and reloads.  this indirectly ensures
    # we have the proper mysql in place for the dump, as the flag to
    # mysqldump also requires a mysqld change that is accompanied in
    # the same RPMs.
    if len(mysql_bin_dir):
      cmdpath = "%s/mysqldump" % mysql_bin_dir
    else:
      cmdpath = "mysqldump"
    if lossy_fp:
      lossy = ''
    else:
      lossy = '--lossless_fp'
    cmd = (("%s --opt -v -u%s -p%s -h%s %s --database %s --tables %s | "
            "gzip --fast > %s/%s-%s.sql.gz; exit ${PIPESTATUS[0]}") %
           (cmdpath, dbspec.user, dbspec.password,
            dbspec.host, lossy, db, table.name, dump_dir, db, table.name))
    pool.submit(cmd, (db, table))

  if not pool.run():
    for failure in pool.failures:
      logging.error("Dump of %s.%s failed: %s" %
                    (failure.data[0], failure.data[1], failure.returncode))
      logging.error("Output: %s\n" % failure.output.read())
    logging.fatal("One or more dumps failed; aborting.")


def GetTableNamesFromDumpFiles(dump_dir):
  logging.info("Get table names from dump files in %s" % dump_dir)
  dump_files = glob.glob(os.path.join(dump_dir, '*.sql.gz'))
  todo = []
  for pathname in dump_files:
    filename = pathname[len(dump_dir) + 1:]
    logging.debug('Found filename %s' % filename)

    match_object = DUMP_RE.match(filename)
    if not match_object or len(match_object.groups()) != 2:
      logging.fatal('Match failed for %s' % filename)

    db = match_object.groups()[0]
    table = match_object.groups()[1]
    logging.info('Found %s.%s' % (db, table))
    todo.append((db, table))
  return todo


def ReloadTables(dbspec, num_writers, dump_dir, mysql_bin_dir, wipe_innodb):
  todo = GetTableNamesFromDumpFiles(dump_dir)

  if wipe_innodb:
    logging.info("Dropping innodb tables...")
    for db, table in todo:
      logging.debug('Drop %s.%s' % (db, table))
      dbspec.execute("DROP TABLE %s.%s" % (db, table))

    logging.info("Stopping mysql...")
    dbspec.stopMySql()

    logging.info("Deleting innodb data files...")
    retcode = subprocess.call("sudo rm -f %s/innodb_data*; "
                              "sudo rm -f %s/innodb_logs/*"
                              % (FLAGS.mysql_root, FLAGS.mysql_root),
                              shell=True)

    if retcode != 0:
      logging.fatal("Error deleting innodb datafiles")

    logging.info("Starting mysql...")
    dbspec.startMySql()
    time.sleep(5)

  logging.info("Restoring tables...")
  pool = command_pool.CommandPool(num_writers)
  if len(mysql_bin_dir):
    cmdpath = "%s/mysql" % mysql_bin_dir
  else:
    cmdpath = "mysql"

  for db, table in todo:
    cmd = (("zcat %s/%s-%s.sql.gz | %s -u%s -p%s -h%s -A %s") %
           (dump_dir, db, table, cmdpath,
            dbspec.user, dbspec.password, dbspec.host, db,
            ))
    logging.debug('Submit %s' % cmd)
    pool.submit(cmd, (db, table))

  if not pool.run():
    for failure in pool.failures:
      logging.error("Restore of %s.%s failed: %s" %
                    (failure.data[0], failure.data[1], failure.returncode))
      logging.error("Output: %s\n" % failure.output.read())
    logging.fatal("One or more restores failed; aborting.")


def main(argv):
  if len(argv) != 2:
    flags.ShowUsage()
    sys.exit(1)

  dbspec = dbspec_lib.DatabaseSpec(argv[1], FLAGS.user, FLAGS.password)

  if not os.path.exists(FLAGS.dump_dir):
    logging.fatal("Specified --dump_dir, %s, does not exist" % FLAGS.dump_dir)
  if not os.path.isdir(FLAGS.dump_dir):
    logging.fatal("Specified --dump_dir, %s, is not a directory" %
                  FLAGS.dump_dir)
  if os.listdir(FLAGS.dump_dir) and FLAGS.do_dump:
    logging.fatal("Specified --dump_dir, %s, is not empty" % FLAGS.dump_dir)

  if _CheckReplicationConfigured(dbspec):
    logging.fatal("Replication must not be configured during a compaction")

  # ensure we are running a mysql that properly lets us dump and
  # restore floating point numbers
  rows = dbspec.execute("SELECT IEEE754_TO_STRING(1.000)", failOnError=0)
  if not rows:
    if not FLAGS.tolerate_lossy_fp:
      logging.fatal("Your version of mysql does not support precise "
                    "IEEE754 string representations.  Dump aborted.")
    else:
      logging.error("Your version of mysql does not support precise "
                    "IEEE754 string representations.")
  else:
    logging.info("Your mysql supports IEEE754_TO_STRING, dump proceding")

  if FLAGS.do_dump:
    DumpTables(dbspec, FLAGS.num_readers, FLAGS.dump_dir, FLAGS.mysql_bin_dir,
               FLAGS.tolerate_lossy_fp)

  if FLAGS.do_reload:
    ReloadTables(dbspec, FLAGS.num_writers, FLAGS.dump_dir, FLAGS.mysql_bin_dir,
                 FLAGS.wipe_innodb)

  logging.info("Done!")


if __name__ == "__main__":
  new_argv = flags.ParseArgs(sys.argv[1:])
  main([sys.argv[0]] + new_argv)
