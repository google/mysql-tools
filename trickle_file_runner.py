#!/usr/bin/python2
#
# Copyright 2006 Google Inc. All Rights Reserved.
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

"""A tool for applying a file of SQL statements to a database.

Usage: trickle_file_runner --dbspec host:user:pass:db --input FILENAME \\
                         [ --state_database DB ] [ --state_table TABLENAME ] \\
                         [ --utilization_percent D ] [ --cycle_time C ]

This utility will take a given file of SQL statements and apply them
to a database in such a way as to guarantee they are applied in order,
even across restarts.  It also guarantees no statement will be
executed twice or skipped.  It will throttle itself, depending upon
cycle time and utilization percent.

When executing the given file, groups of statements in the SQL file
will be merged into transactions whose size varies depending on the
trickle rate.  If the file does not contain BEGIN/COMMIT blocks, then
each line is considered a standalone statement.  If it does contain
such blocks, then those blocks will be executed in their entirety
(though they will likely be grouped in with other such blocks inside
of a transaction).
"""

__author__ = 'bbiskebo@google.com (Brian Biskeborn)'
# Original author: chip@google.com (Chip Turner)

import hashlib
import logging
import re

import gflags

from pylib import app
from pylib import db
from pylib import trickle_lib


FLAGS = gflags.FLAGS

gflags.DEFINE_string('state_database', 'admin',
                     'database name to store state table in (i.e., "scratch")')
gflags.DEFINE_string('state_table', 'TrickleFileState',
                     'table to store state in')
gflags.DEFINE_string('dbspec', '', 'database to execute statements in')
gflags.DEFINE_string('input', '', 'filename to process as input')
gflags.DEFINE_integer('cycle_time', 5,
                      'seconds each batch of statements should take')
gflags.DEFINE_integer('utilization_percent', 10,
                      'target time per cycle to be executing statements rows')
gflags.DEFINE_integer('artificial_batch_cap', None,
                      'never run more than this many lines per batch')
gflags.DEFINE_boolean('init_state_table', False,
                      'populate the state table with file checksums and exit')
gflags.DEFINE_boolean('allow_warnings', False,
                      'continue after sql statements that give warnings')
gflags.DEFINE_string('session_init', None,
                     'SQL commands to run to initialize the session, '
                     'e.g. SET LOG_BIN=0')

# regexes we use to help identify transaction blocks
valid_sql_re = re.compile(
    r'(INSERT (IGNORE )?INTO|REPLACE INTO|UPDATE|DELETE)\s', re.IGNORECASE)
begin_re = re.compile(r'^BEGIN;?\s*$', re.IGNORECASE)
commit_re = re.compile(r'^COMMIT;?\s*$', re.IGNORECASE)


def ValidSqlLine(line):
  """Verify that a given line of text contains valid SQL for the filerunner.

  For our purposes, valid means one of:
  BEGIN, COMMIT, INSERT INTO, INSERT IGNORE INTO, REPLACE INTO,
  DELETE FROM, UPDATE.

  Args:
    line: string to validate
  Returns:
    Boolean, True if the line is valid.
  """
  return (valid_sql_re.match(line) or
          begin_re.match(line) or
          commit_re.match(line))


def _PrepareFile(filename):
  """Open a file, validate it and compute some identifying information.

  Args:
    filename: file to work on
  Returns:
    A file handle, the file size, and the file's SHA1 checksum.
    The file handle is positioned at end of file.
  """
  fh = open(filename, 'r')
  checksummer = hashlib.sha1()
  filesize = 0

  for line in fh:
    if not ValidSqlLine(line):
      logging.fatal('File contains invalid sql: %s', line)

    filesize += len(line)
    checksummer.update(line)

  return fh, filesize, checksummer.hexdigest()


def InitStateTable(dbh, state_db, state_table, fh, fsize, fchecksum):
  """Add file information to the state table.

  If a row already exists for the specified file name, that row is replaced.
  """
  dbh.ExecuteOrDie('REPLACE INTO %s.%s VALUES ("%s", %d, "%s", 0, NULL)' %
                   (state_db, state_table, fh.name, fsize, fchecksum))


class DbFileTrickler(trickle_lib.TrickledOperation):
  """A class to represent trickling a file of SQL statements into a database."""

  def __init__(self, dbh, utilization_percent, cycle_time,
               state_database, state_table, fh, size, checksum):
    """Constructor. Use _PrepareFile() to get the file handle/size/checksum.

    Args:
      dbh: pylib.db handle to the database
      utilization_percent: percent of database time to try to use
      cycle_time: duration of a cycle of insert/sleeps
      state_database: database name to store the state table in
      state_table: table name to store our state in
      fh: handle to the file to trickle statements from
      size: size of the open file in bytes
      checksum: checksum of the open file
    """
    trickle_lib.TrickledOperation.__init__(self, utilization_percent,
                                           cycle_time)

    self._db = dbh
    self._filename = fh.name
    self._fh = fh
    self._size = size
    self._checksum = checksum
    self._offset_bytes = 0
    self._state_database = state_database
    self._state_table = state_table
    self._VerifyStateDatabase()

  def _GetProgress(self):
    """Report our progress for use in status messages."""
    return "%d%% done" % (self._offset_bytes * 100 / self._size)

  def _VerifyStateDatabase(self):
    """Check that the state table exists and create an entry for our input file.

    If an entry for the input file already exists, verify its size and
    checksum.
    """
    rows = self._db.ExecuteOrDie(
        'SELECT Checksum, Size, Offset FROM %s.%s WHERE Filename = "%s"' %
        (self._state_database, self._state_table, self._filename))

    if not rows:
      logging.info('Creating row in state table')
      with self._db.Transaction():
        self._db.ExecuteOrDie(
            'INSERT INTO %s.%s VALUES ("%s", %d, "%s", 0, NULL)' %
            (self._state_database, self._state_table, self._filename,
             self._size, self._checksum))
    else:
      if self._size != long(rows[0]['Size']):
        logging.fatal('database filesize does not match actual file: %s vs %s',
                      self._size, rows[0]['Size'])
      if self._checksum != rows[0]['Checksum']:
        logging.fatal('SHA-1 checksum mismatch on file vs database')
      self._offset_bytes = rows[0]['Offset']
      logging.info('Resuming at offset %d', self._offset_bytes)

  def _SetupTrickle(self):
    """Determine the current file offset based on our state table.

    Called once before the first call to _PerformTrickle().
    """
    row = self._db.ExecuteOrDie(
        'SELECT Offset FROM %s.%s WHERE Filename = "%s"' %
        (self._state_database, self._state_table, self._filename))
    assert len(row) == 1 and len(row[0]) == 1

    self._offset_bytes = long(row[0]['Offset'])

  def _FinalizeTrickle(self):
    pass

  def _Finished(self):
    """Have we finished processing the file?"""
    return self._offset_bytes == self._size

  def _PerformTrickle(self, batch_size):
    """Execute batch_size blocks and update the state table in a transaction.

    Args:
      batch_size: number of blocks to execute. A block is either a single line
                  in the input file, or a group of lines between BEGIN/COMMIT
                  statements.
    Returns:
      Number of blocks actually executed.
    """
    starting_offset = self._offset_bytes
    self._fh.seek(self._offset_bytes)

    # pre-read the data to avoid interleaving disk reads with database writes.
    #
    # also, break the input file into batches.  if we see a BEGIN then
    # we must read through to the next COMMIT even if we would be
    # bigger than batch size.

    # batch is a list of lists.  each entry represents a transaction
    # (ie, list of statements) to be applied to the database.
    batch = []
    while len(batch) < batch_size:
      line = self._fh.readline()
      if not line:
        break

      if not ValidSqlLine(line):
        logging.fatal('Encountered invalid sql: %s', line)

      if begin_re.match(line):
        transaction = []
        while 1:
          next_line = self._fh.readline()

          if not next_line:
            logging.fatal('Input file terminated inside of BEGIN block')
          elif not ValidSqlLine(next_line):
            logging.fatal('Encountered invalid sql: %s', next_line)
          elif begin_re.match(next_line):
            logging.fatal('Attempt to nest transactions')
          elif commit_re.match(next_line):
            break
          else:
            transaction.append(next_line)
        batch.append(transaction)
      elif commit_re.match(line):
        logging.fatal('Attempt to commit outside of transaction')
      else:
        batch.append([line])

    self._offset_bytes = self._fh.tell()
    with self._db.Transaction():
      for transaction in batch:
        for statement in transaction:
          try:
            self._db.ExecuteOrDie(statement)
          except db.QueryWarningsException as e:
            logging.warn('SQL generated warning: %s, %s', str(e), statement)
            if not FLAGS.allow_warnings:
              raise

      result = self._db.ExecuteOrDie(
          'UPDATE %s.%s SET Offset = %d WHERE Filename = "%s" AND Offset = %d' %
          (self._state_database, self._state_table,
           self._offset_bytes, self._filename, starting_offset))
      if result.GetRowsAffected() != 1:
        logging.fatal('Attempt to update database state but something '
                      'already changed it')

    return len(batch)


def main(unused_argv):
  if not (FLAGS.state_database and FLAGS.dbspec and FLAGS.input):
    app.usage(shorthelp=1)
    return 1

  logging.info('Checking file %s', FLAGS.input)
  fh, size, checksum = _PrepareFile(FLAGS.input)

  dbspec = db.Spec.Parse(FLAGS.dbspec)
  with dbspec.Connect() as dbh:
    if FLAGS.session_init:
      logging.info('Initializing session with: %s', FLAGS.session_init)
      dbh.ExecuteOrDie(FLAGS.session_init)

    if FLAGS.init_state_table:
      logging.info('Setting up the state table')
      InitStateTable(dbh, FLAGS.state_database, FLAGS.state_table,
                     fh, size, checksum)
    else:
      logging.info('Starting the trickle')
      trickler = DbFileTrickler(dbh, FLAGS.utilization_percent,
                                FLAGS.cycle_time, FLAGS.state_database,
                                FLAGS.state_table, fh, size, checksum)
      if FLAGS.artificial_batch_cap:
        trickler.SetBatchSizeLimit(FLAGS.artificial_batch_cap)
      trickler.Trickle()

  logging.info('Done')


if __name__ == '__main__':
  FLAGS.logbuflevel = -1
  app.run()
