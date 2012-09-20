#!/usr/bin/python2.6
# Copyright 2012 Google Inc. All Rights Reserved.

"""Read CSV, print a table."""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import csv
import sys

from pylib import app
from pylib import db


def main(argv):
  input_csv = csv.reader(sys.stdin)

  table = db.VirtualTable(input_csv.next(), input_csv)

  for line in table.GetTable():
    print line


if __name__ == '__main__':
  app.run()
