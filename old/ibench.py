#!/usr/bin/python2.6
#
# Copyright (C) 2009 Google Inc.
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

"""Implements a modified version of the insert benchmark as defined by Tokutek.

   A typical command line is:
     ibench.py --db_user=foo --db_password=bar --max_rows=1000000000

   Results are printed after each rows_per_reports rows are inserted.
   The output is:
     Legend:
       #rows = total number of rows inserted
       #seconds = total number of seconds the test has run
       cum_ips = #rows / #seconds
       last_ips = #rows / #seconds for the last rows_per_report rows
       #queries = total number of queries
       cum_qps = #queries / #seconds
       last_ips = #queries / #seconds for the last rows_per_report rows
       #rows #seconds cum_ips last_ips #queries cum_qps last_qps
     1000000 895 1118 1118 5990 5990 7 7
     2000000 1897 1054 998 53488 47498 28 47

  The insert benchmark is defined at http://blogs.tokutek.com/tokuview/iibench

  This differs with the original by running queries concurrent with the inserts.
  For procesess are started and each is assigned one of the indexes. Each
  process then runs index-only queries in a loop that scan and fetch data
  from rows_per_query index entries.

  This depends on multiprocessing which is only in Python 2.6. Backports to
  2.4 and 2.5 are at http://code.google.com/p/python-multiprocessing
"""

__author__ = 'Mark Callaghan'

import MySQLdb
from multiprocessing import Process, Pipe
import optparse
import time
import random
import sys

#
# flags module, on loan from gmt module by Chip Turner.
#

FLAGS = optparse.Values()
parser = optparse.OptionParser()

def DEFINE_string(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(type="string", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

def DEFINE_integer(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(type="int", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

def DEFINE_boolean(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(action="store_true", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

def ParseArgs(argv):
  usage = sys.modules["__main__"].__doc__
  parser.set_usage(usage)
  unused_flags, new_argv = parser.parse_args(args=argv, values=FLAGS)
  return new_argv

def ShowUsage():
  parser.print_help()

#
# options
#

DEFINE_string('engine', 'innodb', 'Storage engine for the table')
DEFINE_string('db_name', 'test', 'Name of database for the test')
DEFINE_string('db_user', 'root', 'DB user for the test')
DEFINE_string('db_password', '', 'DB password for the test')
DEFINE_string('db_host', 'localhost', 'Hostname for the test')
DEFINE_integer('rows_per_commit', 1000, '#rows per transaction')
DEFINE_integer('rows_per_report', 1000000,
               '#rows per progress report printed to stdout. If this '
               'is too small, some rates may be negative.')
DEFINE_integer('rows_per_query', 1000,
               'Number of rows per to fetch per query. Each query '
               'thread does one query per insert.')
DEFINE_integer('cashregisters', 1000, '# cash registers')
DEFINE_integer('products', 10000, '# products')
DEFINE_integer('customers', 100000, '# customers')
DEFINE_integer('max_price', 500, 'Maximum value for price column')
DEFINE_integer('max_rows', 10000, 'Number of rows to insert')
DEFINE_boolean('insert_only', False,
               'When True, only run the insert thread. Otherwise, '
               'start 4 threads to do queries.')
DEFINE_string('table_name', 'purchases_index',
              'Name of table to use')
DEFINE_boolean('setup', False,
               'Create table. Drop and recreate if it exists.')
DEFINE_integer('warmup', 0, 'TODO')

#
# ibench
#

def get_conn():
  return MySQLdb.connect(host=FLAGS.db_host, user=FLAGS.db_user,
                         db=FLAGS.db_name, passwd=FLAGS.db_password)

def create_table():
  conn = get_conn()
  cursor = conn.cursor()
  cursor.execute('drop table if exists %s' % FLAGS.table_name)
  cursor.execute('create table %s ( '
                 'transactionid int not null auto_increment, '
                 'dateandtime datetime, '
                 'cashregisterid int not null, '
                 'customerid int not null, '
                 'productid int not null, '
                 'price float not null, '
                 'primary key (transactionid), '
                 'key marketsegment (price, customerid), '
                 'key registersegment (cashregisterid, price, customerid), '
                 'key pdc (price, dateandtime, customerid)) '
                 'engine=%s' % (FLAGS.table_name, FLAGS.engine))
  cursor.close()
  conn.close()

def get_max_pk(conn):
  cursor = conn.cursor()
  cursor.execute('select max(transactionid) from %s' % FLAGS.table_name)
  max_pk = int(cursor.fetchall()[0][0])
  cursor.close()
  return max_pk

def generate_cols():
  cashregisterid = random.randrange(0, FLAGS.cashregisters)
  productid = random.randrange(0, FLAGS.products)
  customerid = random.randrange(0, FLAGS.customers)
  price = ((random.random() * FLAGS.max_price) + customerid) / 100.0
  return cashregisterid, productid, customerid, price

def generate_row(datetime):
  cashregisterid, productid, customerid, price = generate_cols()
  res = '("%s",%d,%d,%d,%.2f)' % (
      datetime,cashregisterid,customerid,productid,price)
  return res

def generate_pdc_query(row_count, start_time):
  customerid = random.randrange(0, FLAGS.customers)
  price = ((random.random() * FLAGS.max_price) + customerid) / 100.0

  random_time = ((time.time() - start_time) * random.random()) + start_time
  when = random_time + (random.randrange(max(row_count,1)) / 100000.0)
  datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(when))

  sql = 'SELECT price,dateandtime,customerid FROM %s FORCE INDEX (pdc) WHERE '\
        '(price=%.2f and dateandtime="%s" and customerid>=%d) OR '\
        '(price=%.2f and dateandtime>"%s") OR '\
        '(price>%.2f) LIMIT %d' % (FLAGS.table_name, price,
                                   datetime, customerid,
                                   price, datetime, price,
                                   FLAGS.rows_per_query)
  return sql

def generate_pk_query(row_count, start_time):
  sql = 'SELECT transactionid FROM %s WHERE '\
        '(transactionid >= %d) LIMIT %d' % (
      FLAGS.table_name, random.randrange(max(row_count, 1)),
      FLAGS.rows_per_query)
  return sql

def generate_market_query(row_count, start_time):
  customerid = random.randrange(0, FLAGS.customers)
  price = ((random.random() * FLAGS.max_price) + customerid) / 100.0

  sql = 'SELECT price,customerid FROM %s FORCE INDEX (marketsegment) WHERE '\
        '(price=%.2f and customerid>=%d) OR '\
        '(price>%.2f) LIMIT %d' % (
      FLAGS.table_name, price, customerid, price, FLAGS.rows_per_query)
  return sql

def generate_register_query(row_count, start_time):
  customerid = random.randrange(0, FLAGS.customers)
  price = ((random.random() * FLAGS.max_price) + customerid) / 100.0
  cashregisterid = random.randrange(0, FLAGS.cashregisters)

  sql = 'SELECT cashregisterid,price,customerid FROM %s '\
        'FORCE INDEX (registersegment) WHERE '\
        '(cashregisterid=%d and price=%.2f and customerid>=%d) OR '\
        '(cashregisterid=%d and price>%.2f) OR '\
        '(cashregisterid>%d) LIMIT %d' % (
      FLAGS.table_name, cashregisterid, price, customerid,
      cashregisterid, price, cashregisterid, FLAGS.rows_per_query)
  return sql

def generate_insert_rows(row_count):
  when = time.time() + (row_count / 100000.0)
  datetime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(when))
  rows = [generate_row(datetime) for i in xrange(FLAGS.rows_per_commit)]
  return ',\n'.join(rows)

def Query(max_pk, query_func, pipe):
  db_conn = get_conn()

  row_count = max_pk
  start_time = time.time()
  loops = 0
  interval = 10

  while True:
    query = query_func(row_count, start_time)
    cursor = db_conn.cursor()
    cursor.execute(query)
    count = len(cursor.fetchall())
    cursor.close()
    loops += 1
    if (loops % interval) == 0:
      while pipe.poll(0):
        interval, row_count = pipe.recv()
        interval = max(interval, 1)
      pipe.send(loops)

  db_conn.close()

def get_latest(pipes, row_count):
  total = 0
  for p in pipes:
    count = 0
    num_read = 0
    while p.poll(0):
      count = p.recv()
      num_read += 1
    p.send((num_read, row_count))
    total += count
  return total

def Insert(rounds, max_pk, pdc_recv, pk_recv, market_recv, register_recv):
  db_conn = get_conn()
  start_time = time.time()
  prev_time = start_time
  inserted = 0
  recv_pipes = [pdc_recv, pk_recv, market_recv, register_recv]
  prev_sum = 0

  for r in xrange(rounds):
    rows = generate_insert_rows(max_pk + inserted)
    cursor = db_conn.cursor()
    sql = ('insert into %s '
           '(dateandtime,cashregisterid,customerid,productid,price) '
           'values %s' % (FLAGS.table_name, rows))
    cursor.execute(sql)
    cursor.close()

    inserted += FLAGS.rows_per_commit

    if (inserted % FLAGS.rows_per_report) == 0:
      now = time.time()
      sum_queries = get_latest(recv_pipes, max_pk + inserted)
      print '%d %.0f %.1f %.1f %.0f %.1f %.1f' % (
          inserted,
          now - start_time,
          inserted / (now - start_time),
          FLAGS.rows_per_report / (now - prev_time),
          sum_queries,
          sum_queries / (now - start_time),
          (sum_queries - prev_sum) / (now - prev_time))
      sys.stdout.flush()
      prev_time = now
      prev_sum = sum_queries

  db_conn.close()

def run_benchmark():
  rounds = FLAGS.max_rows / FLAGS.rows_per_commit

  if FLAGS.setup:
    create_table()
    max_pk = 0
  else:
    conn = get_conn()
    max_pk = get_max_pk(conn)
    conn.close()

  (pdc_recv,pdc_send) = Pipe(True)
  (pk_recv,pk_send) = Pipe(True)
  (market_recv,market_send) = Pipe(True)
  (register_recv,register_send) = Pipe(True)

  inserter = Process(target=Insert,
                     args=(rounds, max_pk, pdc_recv, pk_recv, market_recv, register_recv))

  if not FLAGS.insert_only:
    query_pdc = Process(target=Query, args=(max_pk, generate_pdc_query, pdc_send))
    query_pk = Process(target=Query, args=(max_pk, generate_pk_query, pk_send))
    query_market = Process(target=Query, args=(max_pk, generate_market_query, market_send))
    query_register = Process(target=Query,
                             args=(max_pk, generate_register_query, register_send))

  inserter.start()

  if not FLAGS.insert_only:
    query_pdc.start()
    query_pk.start()
    query_market.start()
    query_register.start()

  inserter.join()

  if not FLAGS.insert_only:
    query_pdc.terminate()
    query_pk.terminate()
    query_market.terminate()
    query_register.terminate()

  print 'Done'

def main(argv):
  print '#rows #seconds cum_ips last_ips #queries cum_qps last_qps'
  run_benchmark()
  return 0

if __name__ == '__main__':
  new_argv = ParseArgs(sys.argv[1:])
  sys.exit(main([sys.argv[0]] + new_argv))
