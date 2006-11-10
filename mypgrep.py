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

"""mypgrep [ --user USER ] [ --password PASSWORD ] \\
               [ criteria ] host1 host2 host3 ...

A tool to view the sessions across multiple MySQL servers.
"""

__author__ = "chip@google.com (Chip Turner)"

import fcntl
import MySQLdb
import os
import Queue
import re
import signal
import socket
import struct
import sys
import termios
import threading
import time

import thread_pool
import config_helper
config_helper.Init() # here instead of __main__ because we need the
                     # defaults for our string defines

# comaptibility layer with a google module
import compat_flags as flags

FLAGS = flags.FLAGS
flags.DEFINE_string("user", config_helper.GetGlobal("user"),
                    "User to connect to the databases as",
                    short_name="u")
flags.DEFINE_string("password", config_helper.GetGlobal("password") or "",
                    "Password to use when connecting as user",
                    short_name="p")
flags.DEFINE_boolean("show_full_query", 0,
                     "Show the full query when displaying sessions")
flags.DEFINE_boolean("show_summary", 0,
                     "Show a summarization of all matching jobs")
flags.DEFINE_boolean("dns_lookup", 0,
                     "Perform a DNS reverse lookup on IP addresses")
flags.DEFINE_boolean("hide_source", 0,
                     "Hide the database name in the list of results")

flags.DEFINE_boolean("idle", 0, "Show only idle connections")
flags.DEFINE_boolean("busy", 0, "Show only non-idle connections")
flags.DEFINE_boolean("connectionless", 0, "Show only connection-less sessions")
flags.DEFINE_integer("time", 0, "Show sessions whose current activity "
                     "is at least this old (in seconds)")
flags.DEFINE_string("source_user", "", "Show only sessions for this user")
flags.DEFINE_string("source_host", "", "Show only sessions from this host")
flags.DEFINE_string("query_contains", "",
                    "Show only sessions whose query contains this string")
flags.DEFINE_string("query_matches", "",
                    "Show only sessions whose query matches this regex")
flags.DEFINE_string("query_state", "",
                    "Show only sessions whose query state contains this "
                    "(such as 'Sending data' or 'Locked'")
flags.DEFINE_boolean("mine", 0,
                     "Show queries running from this host")
flags.DEFINE_boolean("include_replication", 0,
                     "Include replication threads when matching queries")
flags.DEFINE_boolean("show_state", 0,
                     "Prepend thread state to every query")

flags.DEFINE_boolean("kill", 0,
                     "Kill matching queries (BE CAREFUL!)")
flags.DEFINE_boolean("kill_without_confirm", False,
                     "Go ahead and kill without asking for confirmation")

flags.DEFINE_integer("num_worker_threads", 32,
                     "number of concurrent worker threads")

class SessionEntry:
  """A simple class to represent a result from MySQL's 'show full
     processlist'.

     Little more than a data structure that marginally cleans up data.
  """
  def __init__(self, dbhost, dbh, pid, user, host, db, command, time,
               state, info, raw_host):
    """Constructor.

       Args:
         dbhost: hostname of the database the session is running on
         dbh: database handle connected to this database
         pid: id of the running session
         user: user the session is running as
         host: host the session is running from
         db: database the session is attached to
         command: current MySQL command (aka state) such as Sleep, Query, etc
         time: time the session has been running the current command
         state: special message such as replication state
         info: depends on command; usually the running query
         raw_host: raw ip:port the session is running from

       Returns: n/a
    """
    # helper function to cleanup strings
    def cleanup_string(s):
      if s is None:
        return "(none)"
      else:
        return re.sub(r"\s+", " ", s.strip())

    # host is either host:port or None or empty string, both
    # signifying internal DB hosts
    if host:
      if host == "localhost":
        port = ""
      else:
        host, port = host.split(":")
    else:
      host = "(local)"
      port = ""

    self.dbhost = dbhost
    self.dbh = dbh
    self.pid = pid
    self.user = user
    self.db = db
    self.command = cleanup_string(command)
    self.host = host
    self.raw_host = raw_host
    self.port = port
    self.time = int(time or 0)
    self.state = cleanup_string(state)
    if state:
      if info:
        if FLAGS.show_state:
          info = "[%s] %s" % (state, info)
      else:
        info = "[%s]" % state
    self.info = cleanup_string(info)

  def kill(self):
    cur = self.dbh.cursor()
    # sometimes a connection is present when we get the list, but
    # disconnects before we can kill it.  we continue on, noting an
    # error, if we encounter such an instance.
    try:
      cur.execute("KILL /* mypgrep */ %s" % self.pid)
    except MySQLdb.OperationalError:
      print ("Unable to kill session %d on %s, continuing..." %
             (self.pid, self.dbhost))

  def display(self, host_width, line_width, hide_source, source_width):
    """A method to display a session in a pleasing, columnar way.

       Args:
         host_width: width to reserve for hostnames in display
         line_width: maximum length a line is allowed to be
         hide_source: whether to hide the database the session is on
         source_width: width to reserve for the source name

       Returns:
         string representing the session
    """
    if hide_source:
      buf = ""
    else:
      buf = "%*s" % (-source_width - 1, self.dbhost)

    buf += "%-9d  %*s %-15s %-8d  %-12s " % (self.pid, -host_width, self.host,
                                             self.user, self.time, self.command)
    # if given a width, truncate the info field (aka running query) to
    # fit, if possible
    if line_width > 0:
      substring_len = line_width - len(buf)
      if substring_len < 0:
        substring_len = 0
      return buf + self.info[:substring_len]
    else:
      return buf + self.info


class SessionFilter:
  """A class to represent a filter applied to MySQL sessions.

     Little more than a data structure and a function to compare a
     session to the filter and decide if it is filtered out or not.
  """
  def __init__(self, idle_only, busy_only, minimum_time,
               query_contains, query_matches, query_state,
               source_user, source_host,
               present_conns):
    """Constructor.

       Args:
         idle_only: only show idle sessions
         busy_only: only show non-idle sessions
         minimum_time: lower limit of a session's time
         query_contains: substring query contains
         query_matches: regex the query would match
         query_state: substring query state contains
         source_user: user running the query
         source_host: host query is coming from
         present_conns: dict of host->client of connections to db

       Returns: n/a
    """
    self.idle_only = idle_only
    self.busy_only = busy_only
    self.minimum_time = minimum_time
    self.query_contains = query_contains
    self.query_matches = query_matches
    self.query_state = query_state
    self.source_user = source_user
    self.source_host = source_host
    self.present_conns = present_conns

  def match(self, session):
    """Takes a session and checks it against the current filter.

       Args:
         session: SessionEntry object to examine

       Returns:
         whether the session is allowed by the filter or not
    """
    if FLAGS.include_replication:
      cmd_list = ["Query", "Connect"]
    else:
      cmd_list = ["Query"]
    if self.present_conns:
      # present_conns means, show only clients who are NOT in
      # present_conns.  however, if raw_host is None, then it is a
      # local thread (such as replication) which otherwise would seem
      # connectionless.  So it does not match this filter.
      if not session.raw_host:
        return 0
      # there is an inherent race condition between the time we do the
      # netstat and the time we do the "show processlist".  15 seconds
      # should be more than sufficient.
      if session.time < 15:
        return 0
      # do we have any host data for this dbhost?  if not, something
      # went wrong with the ssh and we should abort letting the user
      # know something is amiss.  prevents accidental kills.
      if (session.dbhost not in self.present_conns or
          not self.present_conns[session.dbhost]):
        print "No connection data for %s - please confirm 'ssh %s' works!" % \
              (session.dbhost, session.dbhost)
        sys.exit(1)
      if session.raw_host in self.present_conns[session.dbhost]:
        return 0
    if self.idle_only:
      if session.command != "Sleep":
        return 0
    if self.busy_only:
      if session.command not in ("Query", "Killed"):
        return 0
    if self.minimum_time:
      if session.time < self.minimum_time:
        return 0
    if self.query_contains:
      if session.command not in cmd_list:
        return 0
      if session.info.lower().find(self.query_contains.lower()) == -1:
        return 0
    if self.query_matches:
      if session.command not in cmd_list:
        return 0
      if not re.search(self.query_matches, session.info):
        return 0
    if self.query_state:
      if session.state.lower().find(self.query_state.lower()) == -1:
        return 0
    if self.source_user:
      if self.source_user != session.user:
        return 0
    if self.source_host:
      if session.host != self.source_host:
        return 0

    return 1


def SessionListWorker(dbhost, host_results, host_results_lock):
  """Thread handler that fetches a session list and stores the results.

     Args:
       dbhost: host to query
       host_results: dict to store results in
       host_results_lock: lock for the host_results dict

     Returns:
       (nothing)
  """
  try:
    dbh = MySQLdb.connect(host=dbhost, user=FLAGS.user, passwd=FLAGS.password,
                          connect_timeout=30)
  except MySQLdb.OperationalError:
    # skip this unreachable host
    sys.stderr.write('Unable to connect to %s, skipping.\n' % dbhost)
    return
  cursor = dbh.cursor()
  cmd = "SHOW /* mygrep */ FULL PROCESSLIST"
  cursor.execute(cmd)

  sessions = []
  for row in cursor.fetchall():
    # skip our boring selves
    if row[7] == cmd:
      continue
    # force it into a list so we can change row[2] aka the host
    row = list(row)

    # save a copy at the end of row in case we do dns resolution
    row.append(row[2])

    # do a little cleanup and beautification of the host
    if FLAGS.dns_lookup:
      if row[2] and row[2].find(":") != -1:
        host, port = row[2].split(":")
        host = socket.gethostbyaddr(host)[0].split(".")[0]
        row[2] = "%s:%s" % (host, port)
    sessions.append(SessionEntry(dbhost, dbh, *row))

  # update the dict
  try:
    host_results_lock.acquire()
    host_results[dbhost] = sessions
  finally:
    host_results_lock.release()


def ShowSummary(users, hosts, commands, busy_time):
  """Display summary statistics about a group of sessions.

     Args:
       users: dict of (username -> count)
       hosts: dict of (hostname -> count)
       commands: dict of (command -> count)
       busy_time: seconds of time amongst all active sessions

     Returns:
       (none)
  """
  def value_sort(d):
    k = d.keys()
    k.sort(lambda a, b: cmp(d[b], d[a]))
    return k

  sorted_users = value_sort(users)
  sorted_hosts = value_sort(hosts)
  sorted_commands = value_sort(commands)

  def print_dict_summary(label, key_order, d):
    print "%s:" % label
    if key_order:
      max_width = max([ len(s) for s in key_order ])
      for key in key_order:
        print "  %*s: %s" % (-max_width - 1, key, d[key])
    print

  print_dict_summary("Hosts", sorted_hosts, hosts)
  print_dict_summary("Users", sorted_users, users)
  print_dict_summary("Commands", sorted_commands, commands)
  print "Busy time: %d" % busy_time

# turn DEADBEEF:C0DE (ip:port in hex) into ip:port in traditional
# dotted, base 10 notation.
def _decode_raw_addr(raw_addr):
  addr, port = raw_addr.split(":")
  port = int(port, 16)
  octets = []
  for i in range(0, len(addr), 2):
    octets.insert(0, str(int(addr[i:i+2], 16)))
  return "%s:%s" % (".".join(octets), port)

# our main method
def main(argv):
  if len(argv) == 1:
    flags.ShowUsage()
    return 1

  # parallelize to perform the sessionlists
  host_results = {}
  host_results_lock = threading.Lock()

  # expand db aliases to the entire set
  dbhosts = []
  for host in argv[1:]:
    aliases = config_helper.ExpandDatabaseAliases(host)
    dbhosts.extend(aliases)

  # check each address, resolving it to see if we are seeing any DNS
  # round robin involved.
  resolved_dbhosts = []
  for host in dbhosts:
    addresses = socket.getaddrinfo(host, None,
                                   socket.AF_INET, socket.SOCK_STREAM)
    if len(addresses) > 1:
      for address in addresses:
        resolved_dbhosts.append(address[4][0])
    else:
      resolved_dbhosts.append(host)
  dbhosts = resolved_dbhosts

  db_connections = {}
  if FLAGS.connectionless:
    db_fhs = {}
    # we are going to do a poor man's version of parallelism here.
    # fire off all of the popens before trying to read from any of
    # them.  the result is ssh will block when it prints the first
    # line of output, which is fine; at that point we can read them
    # quickly.
    for host in dbhosts:
      db_connections[host] = {}
      # we can't use googlesh because sometimes lines from one ssh mix
      # with another.  we can't use netstat because, on occasion, it
      # truncates output with a warning about bogus output.  so, we
      # use plain ssh to get to the proc file netstat uses, then
      # decode the hex addresses and ports.
      fh = os.popen("nice ssh -o BatchMode=yes -o StrictHostKeyChecking=no "
                    "-x -n "
                    "%s cat /proc/net/tcp 2> /dev/null" % host)
      db_fhs[host] = fh

    for host, fh in db_fhs.items():
      for line in fh.readlines():
        line = line.strip()
        if line.find("local_address") != -1:
          continue
        split_line = line.split()
        raw_local_addr, raw_remote_addr, raw_state = \
                        split_line[1], split_line[2], split_line[3]
        remote_addr = _decode_raw_addr(raw_remote_addr)

        # two hex digits represent numerical values for ESTABLISHED, TIME_WAIT,
        # etc.  01 == ESTABLISHED.  all else is considered not to be a
        # valid connection.
        if raw_state == '01':
          db_connections[host][remote_addr] = 1

  # start a thread pool of at most num_worker_threads
  tp = thread_pool.ThreadPool(min(FLAGS.num_worker_threads, len(dbhosts)))
  tp.Start()
  ops = []
  for host in dbhosts:
    ops.append(tp.Submit(SessionListWorker,
                         args=(host, host_results, host_results_lock)))

  for op in ops:
    op.Wait()

  source_host = FLAGS.source_host

  # If --mine was used, set the source host to the ip address
  # associated with what uname says is our hostname.
  if FLAGS.mine:
    if source_host:
      print "--mine and --source_host are mutually exclusive"
      return 1
    source_host = socket.gethostbyname(os.uname()[1])

  # unless requested not to, guess at the terminal width and use that
  # value for displaying each line
  if FLAGS.show_full_query:
    line_width = -1
  else:
    # try a couple of ugly hacks...
    h, w = struct.unpack("hh", fcntl.ioctl(0, termios.TIOCGWINSZ, "xxxx"))
    line_width = w or int(os.getenv("COLUMNS", "80"))

  session_filter = SessionFilter(FLAGS.idle, FLAGS.busy, FLAGS.time,
                                 FLAGS.query_contains, FLAGS.query_matches,
                                 FLAGS.query_state,
                                 FLAGS.source_user, source_host,
                                 db_connections)

  if not host_results:
    print "No results found; unable to connect?"
    sys.exit(1)

  # compute some widths for beautifying the display
  max_source_host_width = max([ len(s) for s in host_results ])
  max_host_width = 0
  for result in host_results.values():
    for session in result:
      max_host_width = max(max_host_width, len(session.host))

  # aggregate data used for summary display
  users = {}
  hosts = {}
  commands = {}
  busy_time = 0

  # process each host in a tolerable order
  host_order = host_results.keys()
  host_order.sort()
  matched_sessions = []
  for host in host_order:
    sessions = host_results[host]
    for session in sessions:
      if session_filter.match(session):
        users[session.user] = users.get(session.user, 0) + 1
        hosts[session.host] = hosts.get(session.host, 0) + 1
        commands[session.command] = commands.get(session.command, 0) + 1
        if session.command == "Query":
          busy_time += session.time

        if not FLAGS.show_summary:
          print session.display(max_host_width, line_width,
                                FLAGS.hide_source, max_source_host_width)
        if FLAGS.kill and session.user != 'system user':
          matched_sessions.append(session)

  killed = 0
  if FLAGS.kill:
    if not FLAGS.kill_without_confirm and len(matched_sessions) > 0:
      print "Kill the above queries? (y/n)",
      answer = raw_input()
      if answer.upper() not in ('Y', 'YES'):
        print "Aborting!"
        sys.exit(1)

    for session in matched_sessions:
      killed += 1
      session.kill()

  if FLAGS.show_summary:
    ShowSummary(users, hosts, commands, busy_time)
    return 0

  if FLAGS.kill:
    if killed == 1:
      plural = ''
    else:
      plural = 's'
    print
    msg = ""
    if "system user" in users:
      msg = " ('system user' sessions not killed)"
    print "%d session%s killed%s" % (killed, plural, msg)

if __name__ == "__main__":
  new_argv = flags.ParseArgs(sys.argv[1:])
  main([sys.argv[0]] + new_argv)
