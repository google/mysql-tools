#!/usr/bin/python2.6
# Copyright 2011 Google Inc. All Rights Reserved.

"""Aggregate CSV input to CSV output.

Specify names (not indexes) of fields to aggregate using flags. Any fields not
specified are assumed to be part of the key.

Usage:
  aggregate.py --sum=FieldName1 --sum=FieldName2 < in.csv > out.csv

For the input file:

FieldName1,FieldName2,Key1,Key2
10,20,foo,bar
20,40,foo,zig
30,60,foo,bar

This will output:

FieldName1,FieldName2,Key1,Key2
40.0,80.0,foo,bar
20.0,40.0,foo,zig
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import csv
import sys

from pylib import app
from pylib import flags

FLAGS = flags.FLAGS

flags.DEFINE_multistring('max', [], 'Name of column to find maximum')
flags.DEFINE_multistring('min', [], 'Name of column to find minimum')
flags.DEFINE_multistring('sum', [], 'Name of column to sum')
flags.DEFINE_multistring('sort', [], 'Name of column to sort by')


def main(argv):
  input_csv = csv.DictReader(sys.stdin)
  output_csv = csv.DictWriter(sys.stdout, input_csv.fieldnames)

  all_fields = set(input_csv.fieldnames)
  to_sum = set(FLAGS.sum)
  to_min = set(FLAGS.min)
  to_max = set(FLAGS.max)
  agg_fields = to_sum | to_min | to_max
  assert len(agg_fields) == len(to_sum) + len(to_min) + len(to_max)
  assert agg_fields <= all_fields, 'Unknown field name'
  keys = all_fields - agg_fields

  aggregates = {}

  for row in input_csv:
    key = tuple(row[x] for x in keys)

    agg_row = aggregates.get(key, None)
    if agg_row:
      for field in to_sum:
        agg_row[field] += float(row[field])
      for field in to_min:
        agg_row[field] = min(agg_row[field], float(row[field]))
      for field in to_max:
        agg_row[field] = max(agg_row[field], float(row[field]))
    else:
      for field in agg_fields:
        row[field] = float(row[field])
      aggregates[key] = row

  output = aggregates.itervalues()

  for sort_key in reversed(FLAGS.sort):
    output = sorted(output, key=lambda x: x[sort_key])

  output_csv.writerow(dict((x, x) for x in all_fields))
  output_csv.writerows(output)


if __name__ == '__main__':
  app.run()
