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

"""Standard HTTP request handlers for an HTTP server."""

__author__ = 'mqian@google.com (Mike Qian)'


import gc
import linecache
import logging
import os
import re
import sys
import threading
import traceback

import db
import vars_common
import vars_export
import vars_helper


_HTML_HEADER = """<HTML>
<HEAD><TITLE>%s</TITLE></HEAD>
<BODY>
"""


_HTML_FOOTER = """
</BODY>
</HTML>
"""


_HTMLDOC_STYLE = """<style>
span {
  position: relative;
  cursor: hand;
}
span b {
  text-decoration: none;
  color: #CC3300;
}
span div {
  display: none;
  z-index: 999;
  position: absolute;
  top: 2em;
  left: 80px;
  border: 1px solid #3333CC;
  padding: 1px;
  background-color: #CCCCFF;
  font-size: smaller;
  width: 400;
}
span div:first-line {
  text-decoration: underline;
}
span:hover div {
  display: block;
}
body {
  padding-bottom: 100px;
}
</style>
"""


def HandleAbortAbortAbort(request, unused_response):
  """Handles /abortabortabort requests."""
  logging.error('/abortabortabort from %s' % request.remote_addr)
  os.abort()


def HandleDBs(unused_request, response):
  """Renders /dbs page with database connection usage."""
  response.headers.add_header('Content-Type', 'text/plain')
  response.start_response(200)

  for conn in db.CONNECTIONS:
    response.out.write(str(conn()))
    response.out.write('\n')

  response.end_response()


def HandlePyHeaps(unused_request, response):
  """Renders /pyheaps page with Python object memory usage."""
  response.headers.add_header('Content-Type', 'text/plain')
  response.start_response(200)

  count = {}
  mem_used = {}
  for obj in gc.get_objects():
    count[type(obj)] = count.get(type(obj), 0) + 1
    mem_used[type(obj)] = mem_used.get(type(obj), 0) + sys.getsizeof(obj)

  response.out.write('Python objects (total bytes, count, type):\n\n')
  for total_size, type_ in sorted(((v, k) for k, v in mem_used.iteritems()),
                                  reverse=True):
    response.out.write('%12s %12s  %s\n' % (total_size, count[type_], type_))

  response.end_response()


def HandlePyThreads(unused_request, response):
  """Renders /pythreads page with Python thread stacks."""
  response.headers.add_header('Content-Type', 'text/plain')
  response.start_response(200)

  frames = sys._current_frames()
  threads = [(t.ident, t) for t in threading.enumerate()]
  thread_names = dict((tid, '(name: %s) ' % t.getName()) for tid, t in threads)

  response.out.write('Python threads:\n\n')
  for thread_id, frame in frames.items():
    response.out.write('--- Thread %d %sstack: ---\n' %
                       (thread_id, thread_names.get(thread_id, '')))
    frame_tuples = []
    while frame:
      filename = frame.f_code.co_filename
      lineno = frame.f_lineno
      line = linecache.getline(filename, lineno)
      frame_tuples.append((filename, lineno, frame.f_code.co_name, line))
      frame = frame.f_back
    frame_tuples.reverse()
    response.out.write(''.join(traceback.format_list(frame_tuples)))
  response.end_response()


def HandleQuitQuitQuit(request, response, server):
  """Handles /quitquitquit requests."""
  response.headers.add_header('Content-Type', 'text/plain')
  response.start_response(200)
  response.out.write('Shutting down...')
  response.end_response()
  logging.warn('/quitquitquit from %s', request.remote_addr)
  server.Shutdown()


def HandleVars(request, response):
  """Renders /vars page with exported variables."""
  requested_vars = request.get('var', '')
  output = request.get('output', 'html')
  regexes = [re.compile(x) for x in request.get('varregexp', [])]

  if regexes and requested_vars:
    title = 'Request Error'
    error_msg = (
        'ERROR: Can only specify one of "var=" or "varregexp=" parameters')
    response.headers.add_header('Content-Type', 'text/html')
    response.start_response(200)
    response.out.write(_HTML_HEADER % title + error_msg + _HTML_FOOTER)
    response.end_response()
    return

  values = {}
  title = 'All Variables'
  # Filter by specific vars.
  if requested_vars:
    vars_list = requested_vars.split(':')
    if len(vars_list) == 1:
      title = 'Variable: %s' % requested_vars.strip()
    else:
      title = 'Variables: %s' % requested_vars.strip()
    for var in vars_list:
      name = vars_common.CanonicalName(var)
      value = vars_helper.FetchVar(var)
      if value is None:
        values[name] = '(Variable not found)'
      else:
        values[name] = value
  # Filter vars by regexes.
  elif regexes:
    title = 'Matching Variables'
    for k, v in vars_helper.FetchAllVars().iteritems():
      for regex in regexes:
        if regex.match(k):
          values[k] = v
          break
  # Just show all the vars.
  else:
    values = vars_helper.FetchAllVars()

  callback = _OUTPUT_FORMATS.get(output, _OUTPUT_FORMATS['html'])
  callback(response, values, title)


def HandleVarsDoc(request, response):
  """Renders /varsdoc page with documentation for exported variables."""
  title = 'All Variables'
  if request.get('output', '') == 'text':
    response.headers.add_header('Content-Type', 'text/plain; charset=UTF-8')
    response.start_response(200)
    response.out.write(_HTML_HEADER % title)
    response.out.write(vars_export.GetAllDocPlain())
    response.out.write(_HTML_FOOTER)
    response.end_response()
  else:
    response.headers.add_header('Content-Type', 'text/html; charset=UTF-8')
    response.start_response(200)
    response.out.write(_HTML_HEADER % title)
    response.out.write(vars_export.GetAllDocHTML())
    response.out.write(_HTML_FOOTER)
    response.end_response()


def _WritePlainResponse(response, values, unused_title):
  """Write the HTTP response for a /vars?output=text request.

  Args:
    response: The HTTP response object.
    values: A dictionary containing (name, value) mappings to be written.
  """
  content = ''.join(vars_export.GetPairPlain(key, values[key])
                    for key in values)
  if len(values) == 1:
    value = content.split(' ', 1)[1]
    if not value.startswith('map:'):
      content = value
  response.headers.add_header('Content-Type', 'text/plain; charset=UTF-8')
  response.start_response(200)
  response.out.write(content)
  response.end_response()


def _WriteHTMLResponse(response, values, title='All Variables'):
  """Write the HTTP response for a /vars?output=html request.

  Args:
    title: The page title.
    response: The HTTP response object.
    values: A dictionary containing (name, value) mappings to be written.
  """
  content = ''.join(vars_export.GetPairHTML(key, values[key]) for key in values)
  if len(values) == 1:
    value = content.split(' ', 1)[1]
    if not value.startswith('map:'):
      content = value
  response.headers.add_header('Content-Type', 'text/html; charset=UTF-8')
  response.start_response(200)
  response.out.write(_HTML_HEADER % title)
  response.out.write('<tt>\n' + content + '</tt>')
  response.out.write(_HTML_FOOTER)
  response.end_response()


def _WriteHTMLDocResponse(response, values, title='All Variables'):
  """Write the HTTP response for a /vars?output=htmldoc request.

  Args:
    title: The page title.
    response: The HTTP response object.
    values: A dictionary containing (name, value) mappings to be written.
  """
  content = ''.join(vars_export.GetVarDocHTML(var) for var in values)
  response.headers.add_header('Content-Type', 'text/html; charset=UTF-8')
  response.start_response(200)
  response.out.write(_HTML_HEADER % title)
  response.out.write(_HTMLDOC_STYLE)
  response.out.write('<tt>\n' + content + '</tt>')
  response.out.write(_HTML_FOOTER)
  response.end_response()


STANDARD_HANDLERS = {
    '/abortabortabort': HandleAbortAbortAbort,
    '/dbs': HandleDBs,
    '/pyheaps': HandlePyHeaps,
    '/pythreads': HandlePyThreads,
    '/quitquitquit': HandleQuitQuitQuit,
    '/vars': HandleVars,
    '/varsdoc': HandleVarsDoc,
}


_OUTPUT_FORMATS = {
    'text': _WritePlainResponse,
    'html': _WriteHTMLResponse,
    'htmldoc': _WriteHTMLDocResponse,
}


def StandardHandlers(callback, request, response, *args, **kwargs):
  """Handles standard page endpoints, or calls callback."""
  uri = request.path.rstrip('/')
  STANDARD_HANDLERS.get(uri, callback)(request, response, *args, **kwargs)
