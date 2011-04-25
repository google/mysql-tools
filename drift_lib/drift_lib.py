#!/usr/bin/python2.6
#
# Copyright 2007-2011 Google Inc.
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

import time

import drift_policies

class Error(Exception):
  """Default Exception class."""
  pass


class DbChecksummer(object):
  """Submits checksum queries against a database.

  A DbChecksummer performs a checksumming job that submits many consecutive
  queries against a database, until all desired tables have been checksummed.
  An instance can be re-used, by calling ChecksumTables() repeatedly. Most
  input parameters are provided in the "config" dictionary (name -> value) so
  that the class can be easily used as a droid module, or standalone program.

  The DbChecksummer is designed to be used as follows:
  db_checksummer.PrepareToChecksum()
  while db_checksummer.ChecksumQuery():
    time.sleep(db_checksummer.GetNextWait())
  """

  # config is a dictionary from parameter names to values
  def __init__(self, query_func, config, db):
    """Initialize the DbChecksummer class with parameters in config dictionary.

    Args:
      query_func: instance of the mysql_connection droid module Query() or the
                  imitation_droid_connection.Query(). Used to query the db.
      config: dictionary of parameter names to values.
      db: string containing the database name.
    Raises:
      Error: config dictionary does not have all of the proper entries.
    """
    # Assert the required parameters are in the config dictionary
    assert config.get('result_table', None)
    assert config.get('golden_table', None)

    self._Query = query_func  # Capitalized member because it is a function.
    self._config = config
    self._db = db
    self._tables = []
    self._current_table = None

    # Initialize JobStarted
    if not config.get('job_started', None):
      now_result = self._Query('SELECT NOW()')
      if now_result:
        self._job_started = now_result[0]['NOW()']
      else:
        raise Error('MySQL Connection problem: SELECT NOW() failed')
    else:
      self._job_started = config['job_started']

    # Initialize the query scheduler
    if not config.get('scan_rate', None):
      raise Error('Must provide scan_rate parameter')
    self._monitor = drift_policies.ProgressMonitor(config['scan_rate'])

    if config.get('secs_per_query', None):
      self._batch_sizer = drift_policies.FixedQueryTimePolicy(
          self._monitor, config['secs_per_query'])
    elif config.get('rows_per_query', None):
      self._batch_sizer = drift_policies.FixedQuerySizePolicy(
          self._monitor, config['rows_per_query'])
    else:
      raise Error('Must provide secs_per_query or rows_per_query parameter')

    if config.get('utilization', None):
      self._wait_timer = drift_policies.FixedUtilizationPolicy(
          self._monitor, config['utilization'])
    elif config.get('hours_to_run', None):
      self._wait_timer = drift_policies.FixedRunTimePolicy(
          self._monitor, config['hours_to_run'] * 3600)
    else:
      raise Error('Must provide utilization or hours_to_run parameter')

    # Parse table restrictions
    self._skip_tables = config.get('tables_to_skip', '').split(',')
    self._engine_list = config.get('engines_to_check', '').split(',')
    self._table_list = config.get('tables_to_check', '').split(',')
    if '' in self._engine_list:
      self._engine_list.remove('')
    if '' in self._table_list:
      self._table_list.remove('')

  def __getstate__(self):
    """Select which fields get saved when this object is pickled.

    We save almost everything. We exclude the _Query function pointer because
    we have to, and we exclude the _tables list since it is large, and it is
    safer to rebuild when we have a fresh database connection anyway.
    """
    state_dict = {}
    state_dict['_config'] = self._config
    state_dict['_db'] = self._db
    state_dict['_current_table'] = self._current_table
    state_dict['_job_started'] = self._job_started
    state_dict['_monitor'] = self._monitor
    state_dict['_batch_sizer'] = self._batch_sizer
    state_dict['_wait_timer'] = self._wait_timer
    state_dict['_skip_tables'] = self._skip_tables
    state_dict['_engine_list'] = self._engine_list
    state_dict['_table_list'] = self._table_list
    return state_dict

  def SetQueryFunction(self, query_func):
    """After unpickling, we need to set the query function.

    Args:
      query_func: instance of the mysql_connection droid module Query() or the
                  imitation_droid_connection.Query(). Used to query the db.
    """
    self._Query = query_func

  def SetUtilization(self, new_utilization_rate):
    """Change the utilization of the checksum job, even mid-run.

    Args:
      new_utilization_rate: ratio of time to spend checksumming.
    """
    self._wait_timer.SetUtilization(new_utilization_rate)

  def _AnalyzeTables(self):
    """Retrieve db schema information to plan/schedule checksumming job.

    After getting the list of tables, several table filters are applied to
    identify the subset for checksumming, and a monitor is updated with the
    data size estimates so that it can make informed scheduling decisions.
    """
    # Get list of tables by running show table status
    table_stats = self._Query('SHOW TABLE STATUS FROM %s' % self._db)
    if not table_stats:
      raise Error('SHOW TABLE STATUS FROM %s does not return rows.' % self._db)
    # Create and initialize a TableChecksummer for each table
    self._tables = []
    for row in table_stats:

      # Obey specified table restrictions
      if self._engine_list and row['Engine'] not in self._engine_list:
        continue
      if row['Name'] in self._skip_tables:
        continue
      if self._table_list and row['Name'] not in self._table_list:
        continue
      self._tables.append(TableChecksummer(self._Query, self._monitor,
                                           self._batch_sizer, self._wait_timer,
                                           self._config, self._db, row['Name'],
                                           self._job_started))

      self._monitor.AddTable(row['Name'], row['Rows'], row['Data_length'])

  def GetJobStartTime(self):
    """Provides access to the last checksum job start time.

    Returns:
      Empty string if job never started, DD/MM/YYYY HH:MM:SS otherwise
    """
    return self._job_started

  def PrepareToChecksum(self):
    """Call once before starting to call ChecksumQuery repeatedly.

    This function also must be called after unpickling, before making any
    checksum queries.
    """
    self._AnalyzeTables()
    self._position = 0
    if not self._current_table and len(self._tables):
      self._current_table = self._tables[self._position]
    while self._position < len(self._tables):
      if (self._tables[self._position].table_name ==
          self._current_table.table_name):
        # If this is a restore, we use the saved table in _current_table.
        self._tables[self._position] = self._current_table
        self._current_table.SetQueryFunction(self._Query)
        self._current_table.PrepareToChecksum()
        break
      self._position += 1

  def ChecksumQuery(self):
    """Call repeatedly until it returns False to checksum the database.

    Caller should sleep for the returned number of seconds before issuing
    the next ChecksumQuery() call.

    Returns:
      False if checksumming is complete, True if there are more queries to run.
    """
    if self._position >= len(self._tables):
      self._current_table = None
      return False
    if not self._tables[self._position].ChecksumQuery():
      self._position += 1
      if self._position < len(self._tables):
        self._tables[self._position].PrepareToChecksum()
        self._current_table = self._tables[self._position]
    return True

  def GetNextWait(self):
    """Suggests time to sleep before next checksum query."""
    if self._position >= len(self._tables):
      return 0
    return self._tables[self._position].GetNextWait()

  def ReportPerformance(self):
    """Provides information about checksumming progress and performance.

    Returns:
      A string containing checksum performance and progress details.
    """
    return self._monitor.ReportPerformance()

  def ChecksumTables(self):
    """Generate checksums for each table.

    This is the public method that executes an entire checksumming job. It
    starts by analyzing the schema, and doesn't return until the checksumming
    job is complete.
    """
    self._PrepareToChecksum()
    while self.ChecksumQuery():
      time.sleep(self.GetNextWait())


class TableChecksummer(object):
  """Class responsible for checksumming a single table.

  Members:
  table_name is a publicly accessible string containing the table's name.
  """

  def __init__(self, query_func, monitor, batch_sizer, wait_timer, config, db,
               table, job_started):
    """Initialize TableChecksummer with config dictionary.

    Args:
      query_func: instance of the mysql_connection droid module Query function
                  or the imitation_droid_connection class Query function.
      monitor: instance of ProgressMonitor class, tracks progress.
      batch_sizer: subclass of BatchSizePolicy, determines rows per query.
      wait_timer: subclass of WaitTimePolicy, determines time between queries.
      config: dictionary of parameter names to values.
      db: string containing the database name.
      table: string containing the table name.
      job_started: string indicating job start time in MySQL now() format.
    """
    self._Query = query_func  # Capitalized member because it is a function.
    self._monitor = monitor
    self._batch_sizer = batch_sizer
    self._wait_timer = wait_timer
    self._db = db
    self.table_name = table
    self._skip_column_types = config.get('column_types_to_skip', '').split(',')
    self._row_condition = config.get('row_condition', '')
    # The following are required parameters
    self._result_table = config.get('result_table')
    self._golden_table = config.get('golden_table')
    self._job_started = job_started
    self._chunk = 1
    self._prepared = False
    self._query_dict = {}

  def __getstate__(self):
    """Determine which fields to store when pickling.

    The intent is to save only the currently executing table. We save almost
    all members, but we exclude the _Query function pointer because we have to.
    """
    state_dict = {}
    state_dict['_monitor'] = self._monitor
    state_dict['_batch_sizer'] = self._batch_sizer
    state_dict['_wait_timer'] = self._wait_timer
    state_dict['_db'] = self._db
    state_dict['table_name'] = self.table_name
    state_dict['_skip_column_types'] = self._skip_column_types
    state_dict['_row_condition'] = self._row_condition
    state_dict['_result_table'] = self._result_table
    state_dict['_golden_table'] = self._golden_table
    state_dict['_job_started'] = self._job_started
    state_dict['_chunk'] = self._chunk
    state_dict['_query_dict'] = self._query_dict
    state_dict['_prepared'] = False  # We need to re-prepare after reloading
    return state_dict

  def SetQueryFunction(self, query_func):
    """After unpickling, we need to set the query function.

    Args:
      query_func: instance of the mysql_connection droid module Query() or the
                  imitation_droid_connection.Query(). Used to query the db.
    """
    self._Query = query_func

  def PrepareToChecksum(self):
    """Call before making any calls to ChecksumQuery."""
    if not self._InitQueryDict():
      return
    # If we need to start in the middle of the table, we get the starting
    # offsets from _query_dict['offset_copy'] (last query re-inserted).
    prev_offsets = self._query_dict.get('offset_copy', '')
    offset_array = prev_offsets.split(':')  # Key1:RangeStart1:RangeEnd1:Key2..
    keys = []
    for i in range(len(offset_array) - 2):
      if not i % 3:
        key_field = offset_array[i]
        key_value = offset_array[i+2]
        keys.append("%s = '%s'" % (key_field, key_value.replace("'","\\'")))
    if keys:
      self._query_dict['initial_where'] = 'WHERE %s' % ' AND '.join(keys)
    self._Query(TableChecksummer.INITIALIZATION_QUERY % self._query_dict)
    self._prepared = True

  def ChecksumQuery(self):
    """Issues one checksum query against the table.

    Should be called repeatedly until it returns False.
    Caller should call GetNextWait to determine sleep time before next query

    Returns:
      False when the table has completed checksumming, True otherwise.
    """
    if not self._prepared:
      return False
    rows_to_read = self._batch_sizer.GetNextBatchSize(self.table_name)
    self._query_dict['batch_size'] = rows_to_read
    self._query_dict['chunk'] = self._chunk
    self._chunk += 1
    start_time = time.time()
    self._Query('SELECT %(subsequent_assignment)s' % self._query_dict)
    self._Query(TableChecksummer.CHECKSUM_QUERY % self._query_dict)
    count_result = self._Query('SELECT @count')
    if not count_result:  # If we encounter a problem, we give up on this table
      return False
    rows_read = count_result[0]['@count']
    if rows_read is None:
      return False
    self._monitor.RecordProgress(rows_read, time.time() - start_time,
                                 self.table_name)
    self._monitor.ReportPerformance()
    self._ReinsertByValue()
    return rows_read == rows_to_read

  def GetNextWait(self):
    """Seconds the caller should wait before next ChecksumQuery call."""
    return self._wait_timer.GetNextWait(self.table_name)

  def _ReinsertByValue(self):
    """Selects a checksum entry, submits a new insert with the same values."""
    # Returns ChunkDone, Offsets, Checksums, Count
    rows = self._Query(TableChecksummer.GET_CHECKSUM_QUERY % self._query_dict)
    # TODO(benhandy): how should we handle this error?
    if not rows or len(rows) != 1:
      return
    row = rows[0]

    self._query_dict['chunk_done_copy'] = row['ChunkDone']
    self._query_dict['offset_copy'] = row['Offsets']
    self._query_dict['checksum_copy'] = row['Checksums']
    self._query_dict['count_copy'] = row['Count']
    self._Query(TableChecksummer.INSERT_CHECKSUM_QUERY % self._query_dict)

  def _IdentifyColumns(self):
    """Identify primary key and other columns, applying column filters.

    Returns:
      True if the columns and primary key were found, False otherwise.
    """
    rows = self._Query('DESCRIBE %s.%s' % (self._db, self.table_name))
    if not rows:  # First checksum query will fail
      return False

    # Describe table: | Field | Type | Null | Key | Default | Extra |
    self._primary_key = []
    self._columns = []
    for row in rows:
      datatype = row['Type']
      if '(' in datatype:
        datatype = datatype[:datatype.index('(')]  # bigint(20) -> bigint
      if datatype not in self._skip_column_types:
        self._columns.append(row['Field'])

    # Describe table doesn't return primary key in order, use show indexes from
    rows = self._Query('SHOW INDEXES FROM %s.%s' % (self._db, self.table_name))
    if not rows:
      return False
    for row in rows:
      if row['Key_name'] == 'PRIMARY':
        self._primary_key.append(row['Column_name'])

    # If there is no primary key, we will not attempt to checksum the table.
    if not self._primary_key:
      return False
    return True

  def _InitQueryDict(self):
    """Generate table-specific re-usable portions of the query.

    Returns:
      True if the query dictionary is correctly initialized, False otherwise.
    """
    if not self._IdentifyColumns():
      return False
    offset_str = ', '.join(["'%s', @start_%s, @next_%s := last_value(%s)" %
                            (key, key, key, key) for key in self._primary_key])
    offset_str = "concat_ws(':', %s)" % offset_str

    checksum_str = ', '.join(["'%s\', ORDERED_CHECKSUM(%s)" % (col, col)
                              for col in self._columns])
    checksum_str = "concat_ws(':', %s)" % checksum_str
    primary_key_str = ', '.join(key for key in self._primary_key)
    initial_assign_list = ['@next_%s := %s' % (key, key)
                           for key in self._primary_key]
    initial_assign_str = ', '.join(initial_assign_list)
    subsequent_assign_str = ', '.join('@start_%s := @next_%s' %
                                      (key, key) for key in self._primary_key)

    # Tricky python to produce where clause: (p1=c1 and p2>c2) or p1>c1
    and_clauses = []
    for i in range(len(self._primary_key)):
      # This iteration adds and-clause with greater-than on column i
      and_terms = ['%s = @start_%s' % (key, key)
                   for key in self._primary_key[:i]]
      and_terms.append('%s > @start_%s' % (self._primary_key[i],
                                           self._primary_key[i]))
      and_clauses.append('(' + ' and '.join(and_terms) + ')')
    where_str = ' OR '.join(and_clauses)
    if not where_str:
      where_str = 'TRUE'
    if self._row_condition:
      where_str = ' AND '.join(['(' + where_str + ')', self._row_condition])

    self._query_dict.update({'db': self._db,
                             'table': self.table_name,
                             'result_table': self._result_table,
                             'golden_table': self._golden_table,
                             'offsets': offset_str,
                             'checksums': checksum_str,
                             'where': where_str,
                             'primary_key': primary_key_str,
                             'initial_assignment': initial_assign_str,
                             'subsequent_assignment': subsequent_assign_str,
                             'initial_where': '',  # Only for resuming runs
                             'job_started': self._job_started,
                             'chunk': 1,        # Set for each query
                             'batch_size': 1})  # Set for each query
    return True

# SQL Queries
  INITIALIZATION_QUERY = """
SELECT %(initial_assignment)s
FROM %(db)s.%(table)s
%(initial_where)s
ORDER BY %(primary_key)s
LIMIT 1
""".replace('\n', ' ').strip()

  CHECKSUM_QUERY = """
REPLACE INTO %(result_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
SELECT '%(db)s', '%(table)s', %(chunk)s, '%(job_started)s',
NOW(), %(offsets)s, %(checksums)s, @count := count(*)
FROM (SELECT * FROM %(db)s.%(table)s FORCE INDEX (PRIMARY)
WHERE %(where)s
ORDER BY %(primary_key)s
LIMIT %(batch_size)s) f
""".replace('\n', ' ').strip()

  GET_CHECKSUM_QUERY = """
SELECT ChunkDone, Offsets, Checksums, Count
FROM %(result_table)s
WHERE DatabaseName='%(db)s' AND
TableName='%(table)s' AND
Chunk=%(chunk)s AND
JobStarted='%(job_started)s' AND
Count=@count
""".replace('\n', ' ').strip()

  INSERT_CHECKSUM_QUERY = """
REPLACE INTO %(golden_table)s
(DatabaseName, TableName, Chunk, JobStarted, ChunkDone,
Offsets, Checksums, Count)
VALUES ('%(db)s', '%(table)s', %(chunk)s, '%(job_started)s',
'%(chunk_done_copy)s', '%(offset_copy)s', '%(checksum_copy)s',
%(count_copy)s)
""".replace('\n', ' ').strip()
