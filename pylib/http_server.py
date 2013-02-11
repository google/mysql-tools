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

"""Basic HTTP server API.

This API is a hybrid of WSGI and WebOb APIs. It's been modified to make
responding to a request an asynchronous operation, so the application doesn't
have to block a thread if it needs to query a backend.

Example usage:

def Callback(request, response):
  response.add_header('Content-Type', 'text/plain')
  response.start_response(200)
  response.out.write('Your IP address is: %s' % request.remote_addr)
  response.end_response()

server = http_server.Server('0.0.0.0', 9999, Callback)
server.Loop()
"""

import asyncore
import logging
import socket
import threading
import urlparse
import weakref
from wsgiref import headers


class CaseInsensitiveDict(dict):
  """Dictionary in which keys are case-insensitive."""

  def __setitem__(self, key, value):
    dict.__setitem__(self, key.lower(), value)

  def __getitem__(self, key):
    return dict.__getitem__(self, key.lower())


class Request(object):
  """A single HTTP request."""

  def __init__(self, request, remote_addr):
    self.remote_addr = remote_addr
    lines = request.split('\r\n')
    verb_line = lines.pop(0)
    method, path, unused_protocol = verb_line.split(' ', 2)

    self.headers = CaseInsensitiveDict()
    for line in lines:
      key, value = line.split(': ', 1)
      self.headers[key] = value

    self.url = 'http://%s/%s' % (self.headers['host'], path.lstrip('/'))

    self.method = method.upper()
    self.path = path
    self.query_string = ''
    if '?' in self.path:
      self.path, self.query_string = self.path.split('?', 1)

    self._qs = urlparse.parse_qs(self.query_string)
    # TODO(flamingcow): Cookies.

  def get(self, argument_name, default_value=''):
    """Get one value of a query argument by name."""
    return self._qs.get(argument_name, [default_value])[0]

  def get_all(self, argument_name):
    """Get all values of a query argument by name."""
    return self._qs.get(argument_name, [])


class OutFile(object):
  """Chunked HTTP encoding for response output."""

  def __init__(self, connection):
    self._connection = connection

  def write(self, value):
    try:
      self._connection.send('%X\r\n' % len(value))
      self._connection.send(value)
      logging.debug('Server wrote content of length %s', len(value))
      self._connection.send('\r\n')
    except IOError:
      self._connection.close()


class Response(object):
  """Response API for a single request."""

  def __init__(self, connection):
    self._connection = connection
    self.headers = headers.Headers([('Transfer-Encoding', 'chunked')])
    self.out = OutFile(self._connection)

  def start_response(self, code):
    """Send code and headers. Must be run before using Response.out."""
    try:
      self._connection.send('HTTP/1.1 %d %s\r\n' %
                            (code, ResponseCode.GetReasonPhrase(code)))
      self._connection.send(str(self.headers))
    except IOError:
      self._connection.close()

  def end_response(self):
    """Finalize response. Must be run after all other operations."""
    self.out.write('')
    self._connection.ResponseComplete()


class ResponseCode(object):
  """API for retrieval of HTTP status code phrases."""
  _RESPONSE_CODES = {
      # Informational
      100: 'Continue',                          # HTTP/1.1, RFC 2616
      101: 'Switching Protocols',               # HTTP/1.1, RFC 2616
      102: 'Processing',                        # WEBDAV, RFC 2518
      # Successful
      200: 'OK',                                # HTTP/1.1, RFC 2616
      201: 'Created',                           # HTTP/1.1, RFC 2616
      202: 'Accepted',                          # HTTP/1.1, RFC 2616
      203: 'Non-Authoritative Information',     # HTTP/1.1, RFC 2616
      204: 'No Content',                        # HTTP/1.1, RFC 2616
      205: 'Reset Content',                     # HTTP/1.1, RFC 2616
      206: 'Partial Content',                   # HTTP/1.1, RFC 2616
      207: 'Multi-Status',                      # WEBDAV, RFC 2518
      226: 'IM Used',                           # RFC 3229
      # Redirection
      300: 'Multiple Choices',                  # HTTP/1.1, RFC 2616
      301: 'Moved Permanently',                 # HTTP/1.1, RFC 2616
      302: 'Found',                             # HTTP/1.1, RFC 2616
      303: 'See Other',                         # HTTP/1.1, RFC 2616
      304: 'Not Modified',                      # HTTP/1.1, RFC 2616
      305: 'Use Proxy',                         # HTTP/1.1, RFC 2616
      307: 'Temporary Redirect',                # HTTP/1.1, RFC 2616
      # Client Error
      400: 'Bad Request',                       # HTTP/1.1, RFC 2616
      401: 'Unauthorized',                      # HTTP/1.1, RFC 2616
      402: 'Payment Required',                  # HTTP/1.1, RFC 2616
      403: 'Forbidden',                         # HTTP/1.1, RFC 2616
      404: 'Not Found',                         # HTTP/1.1, RFC 2616
      405: 'Method Not Allowed',                # HTTP/1.1, RFC 2616
      406: 'Not Acceptable',                    # HTTP/1.1, RFC 2616
      407: 'Proxy Authentication Required',     # HTTP/1.1, RFC 2616
      408: 'Request Timeout',                   # HTTP/1.1, RFC 2616
      409: 'Conflict',                          # HTTP/1.1, RFC 2616
      410: 'Gone',                              # HTTP/1.1, RFC 2616
      411: 'Length Required',                   # HTTP/1.1, RFC 2616
      412: 'Precondition Failed',               # HTTP/1.1, RFC 2616
      413: 'Request Entity Too Large',          # HTTP/1.1, RFC 2616
      414: 'Request-URI Too Long',              # HTTP/1.1, RFC 2616
      415: 'Unsupported Media Type',            # HTTP/1.1, RFC 2616
      416: 'Requested Range Not Satisfiable',   # HTTP/1.1, RFC 2616
      417: 'Expectation Failed',                # HTTP/1.1, RFC 2616
      422: 'Unprocessable Entity',              # WEBDAV, RFC 2518
      423: 'Locked',                            # WEBDAV, RFC 2518
      424: 'Failed Dependency',                 # WEBDAV, RFC 2518
      426: 'Upgrade Required',                  # RFC 2817
      # Server Error
      500: 'Internal Server Error',             # HTTP/1.1, RFC 2616
      501: 'Not Implemented',                   # HTTP/1.1, RFC 2616
      502: 'Bad Gateway',                       # HTTP/1.1, RFC 2616
      503: 'Service Unavailable',               # HTTP/1.1, RFC 2616
      504: 'Gateway Timeout',                   # HTTP/1.1, RFC 2616
      505: 'HTTP Version Not Supported',        # HTTP/1.1, RFC 2616
      507: 'Insufficient Storage',              # WEBDAV, RFC 2518
      510: 'Not Extended',                      # RFC 2774
  }

  @classmethod
  def GetReasonPhrase(cls, code):
    """Returns the W3C name for a given response code."""
    if not isinstance(code, int):
      raise TypeError('HTTP status code must be an integer: %r' % code)

    if code not in cls._RESPONSE_CODES:
      logging.warn('Unknown HTTP status code: %d', code)
    return cls._RESPONSE_CODES.get(code, 'Response')


class Connection(asyncore.dispatcher_with_send):
  """A client connection."""

  _blocked = False

  def __init__(self, sock, addr, callback):
    asyncore.dispatcher_with_send.__init__(self, sock)
    self._buffer = ''
    self._remote_addr = addr
    self._callback = callback

  def handle_read(self):
    data = self.recv(8192)
    if not data:
      self.close()
      return
    self._buffer += data
    self._CheckForNewRequest()

  def _CheckForNewRequest(self):
    """Check the buffer for a new request."""
    if self._blocked:
      return
    if '\r\n\r\n' in self._buffer:
      request, new_buffer = self._buffer.split('\r\n\r\n', 1)
      # TODO(flamingcow): Support requests with body contents.
      self._buffer = new_buffer
      response = Response(self)
      try:
        request = Request(request, self._remote_addr)
      except (KeyError, ValueError):
        response.headers.add_header('Content-Type', 'text/plain')
        response.start_response(400)
        response.out.write('Failed to parse request.')
        response.end_response()
        return

      if request.path != '/favicon.ico':
        logging.info('[%s] %s %s', self._remote_addr, request.method,
                     request.path)
      self._blocked = True
      self._callback(request, response)

  def ResponseComplete(self):
    """Associated request completed, so we can dispatch another one."""
    self._blocked = False
    self._CheckForNewRequest()


class Server(asyncore.dispatcher):
  """HTTP server that dispatches tasks for asynchronous responses."""

  def __init__(self, host, port, callback):
    asyncore.dispatcher.__init__(self)
    self._callback = callback
    self._started = threading.Event()
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
    self.set_reuse_addr()
    self.bind((host, port))
    self.listen(5)
    self._connections = weakref.WeakValueDictionary()

  def handle_accept(self):
    pair = self.accept()
    if pair is None:
      return
    sock, addr = pair
    logging.debug('Received connection from %s:%s' % addr)
    self._connections[sock] = Connection(sock, addr[0], self._callback)

  def WaitUntilHTTPServerIsRunning(self):
    self._started.wait()

  def Loop(self):
    self._started.set()
    asyncore.loop()

  def Shutdown(self):
    for conn in self._connections.values():
      if conn is not None:
        conn.close()
    self.close()
