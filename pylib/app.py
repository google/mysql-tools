#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc. All Rights Reserved.

"""Wrap an application execution.

Parse flags, handle usage errors.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import sys
import flags


class Error(Exception):
  pass


class UsageError(Error):
  pass


def run():
  try:
    argv = flags.ParseArgs(sys.argv)
    sys.exit(sys.modules['__main__'].main(argv))
  except (UsageError, flags.FlagValidationError) as e:
    print e
    print
    flags.ShowUsage()
    sys.exit(1)
