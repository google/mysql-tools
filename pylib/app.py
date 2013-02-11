"""Wrap an application execution.

Parse flags, handle usage errors.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import sys

import gflags

FLAGS = gflags.FLAGS

class Error(Exception):
  pass


class UsageError(Error):
  pass


def run():
  try:
    argv = FLAGS(sys.argv)
    sys.exit(sys.modules['__main__'].main(argv))
  except (UsageError, gflags.FlagsError) as err:
    print '%s\nUsage: %s ARGS\n%s' % (
        err, sys.argv, FLAGS)
    sys.exit(1)
