#!/usr/bin/python2.6
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

"""A class to act as a command line flag parser.

Internally, this uses optparse.
"""

__author__ = 'chip@google.com (Chip Turner)'

import optparse
import sys

FLAGS = optparse.Values()
parser = optparse.OptionParser()
CALLS = []

def RecordCall(func):
  def wrapper(*args, **kwargs):
    CALLS.append((func.__name__, args, kwargs))
    return func(*args, **kwargs)
  return wrapper

@RecordCall
def DEFINE_string(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(type="string", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

@RecordCall
def DEFINE_integer(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(type="int", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

@RecordCall
def DEFINE_float(name, default, description, short_name=None):
  if default is not None and default != '':
    description = "%s (default: %s)" % (description, default)
  args = [ "--%s" % name ]
  if short_name is not None:
    args.insert(0, "-%s" % short_name)

  parser.add_option(type="float", help=description, *args)
  parser.set_default(name, default)
  setattr(FLAGS, name, default)

@RecordCall
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
