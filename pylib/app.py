#!/usr/bin/python2.4
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Wrap an application execution.

Parse flags, handle usage errors.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import sys
import flags


class UsageError(Exception):
  pass


def run():
  argv = flags.ParseArgs(sys.argv)
  try:
    sys.exit(sys.modules['__main__'].main(argv))
  except UsageError:
    flags.ShowUsage()
    sys.exit(1)
