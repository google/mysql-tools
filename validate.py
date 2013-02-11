#!/usr/bin/python2
#
# Copyright 2011 Google Inc. All Rights Reserved.
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

"""Utility to parser and validate a schema change.

  validate.py FILENAME...
"""

from parser_lib import validator
from pylib import app
from pylib import db
from pylib import schema

import gflags

FLAGS = gflags.FLAGS

gflags.DEFINE_string('db', None, 'DB spec to read from')


def main(argv):
  if not FLAGS.db:
    raise app.UsageError('Please specify --db')
  if len(argv) < 2:
    raise app.UsageError('Please specify at least one filename.')

  dbh = db.Connect(FLAGS.db)
  val = validator.Validator(schema.Schema(dbh))

  for filename in argv[1:]:
    val.ValidateString(open(filename, 'r').read())

  dbh.Close()


if __name__ == '__main__':
  app.run()
