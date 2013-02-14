#!/usr/bin/python2
#
# Copyright 2012 Google Inc. All Rights Reserved.
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

"""Extract slow queries from binary logs.

This scripts expects on stdin the output from mysqlbinlog and prints
on stdout all the queries that take more than a configurable amount of
time.
"""

__author__ = 'razvanm@google.com (Razvan Musaloiu-E.)'

import datetime
import sys
import time

import gflags

from pylib import app

FLAGS = gflags.FLAGS

gflags.DEFINE_integer(
    'cutoff_seconds', 2,
    'Only show transactions that are longer that this value')


def main(unused_argv):
  current_trx = None
  current_ts = None
  prev_ts = None
  query = []

  # A single typical transaction looks like this:
  #
  #  1. # at 385 330500161
  #  2. #120105 14:52:55 server id 242880000  end_log_pos 452
  #       group_id 330500161       Query   thread_id=324436
  #       exec_time=8     error_code=0
  #  3. SET TIMESTAMP=1325803975/*!*/;
  #  4. BEGIN/*!*/;
  #  5. # at 452 330500161
  #  6. #120105 14:52:55 server id 242880000  end_log_pos 637
  #       group_id 330500161       Query   thread_id=324436
  #       exec_time=8     error_code=0
  #  7. use admin/*!*/;
  #  8. SET TIMESTAMP=1325803975/*!*/;
  #  9. UPDATE Heartbeat
  # 10.     SET LastHeartbeat = UNIX_TIMESTAMP()
  # 11.     WHERE LastHeartbeat <= UNIX_TIMESTAMP() - 0.000000/*!*/;
  # 12. # at 637 330500161
  # 13. #120105 14:52:55 server id 242880000  end_log_pos 672
  #        group_id 330500161       Xid = 3037797
  # 14. COMMIT/*!*/;

  for line in sys.stdin:
    if line.startswith('# '):
      ts_line = sys.stdin.next()
      v = ts_line.split()

      if 'Query' in v:
        trx = v[8]
        date = datetime.datetime.strptime(ts_line[1:16], '%y%m%d %H:%M:%S')

        # exec_time can be at slightly different positions.
        exec_time = int(
            [i for i in v if i.startswith('exec_time=')][0].split('=')[1])
        ts = int(time.mktime(date.timetuple())) + exec_time

        if trx != current_trx:
          if current_trx:
            diff = current_ts - prev_ts
            if diff > FLAGS.cutoff_seconds:
              query = ''.join(query)
              print 'timestamp: %d trx: %s delay: %d bytes: %d' % (
                  current_ts, current_trx, diff, len(query))
              print
              print query
              print '*' * 80
              print
          else:
            current_ts = ts

          prev_ts = current_ts
          query = []
        query.append(line)
        query.append(ts_line)
        current_trx = trx
        current_ts = ts
    else:
      query.append(line)

if __name__ == '__main__':
  app.run()
