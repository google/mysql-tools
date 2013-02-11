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

"""Utility to push and publish permissions.

  permissions.py [flags] COMMAND

Usual COMMANDs:
  copy
    Copy the published permissions --set to --db.
  dump
    Dump the SQL for the permissions --set from --file.
  list-sets
    List the permissions set names defined in --file.
  publish
    Publish the permissions set --source_set from --file to --db as --set.
  push
    Push permissions set --source_set from --file directly to --db.
  test
    Test all permissions sets defined in --file.

Note the --db=DBSPEC flag is required for copy/publish/push commands and may
be required for push/test depending on your permissions configuration.
Examples:
  --db=dbhost#:::dbname#
  --db=dbhost#:otheruser::dbname#

Because some people do not trust the strength of mysql hashes,
permissions.py is capable of encrypting them with an RSA key pair.  If
you choose to use this, the following extra COMMANDs will help you
maintain your permissions file:

  hash-password
    Prompt for a password and print its MySQL hash.
  encrypt-password
    Prompt for a password, compute its mysql hash, encrypt that hash with
    --public_keyfile, and print to stdout.
  encrypt-hash
    Prompt for a mysql hash, encrypt it with --public_keyfile, and print to
    stdout.
  decrypt-hash
    Prompt for an encrypted string, decrypt it with --private_keyfile,
    print the results (hopefully a MySQL hash) to stdout.
  generate-key
    Generate encryption keys and write them to --public_keyfile and
    --private_keyfile.
  generate-password
    Generate a random password.
"""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import inspect
import sys

import gflags

from permissions_lib import define
from permissions_lib import use
from permissions_lib import utils
from pylib import app
from pylib import db

FLAGS = gflags.FLAGS

gflags.DEFINE_string('db', None, 'DB spec to read from/push to')
gflags.DEFINE_integer('push_duration', None,
                      'Staggered push duration (seconds)')
gflags.DEFINE_multistring('file', None,
                          'File containing permissions settings on which to '
                          'operate. The file may contain one or more named sets '
                          'of permissions.')
gflags.DEFINE_string('public_keyfile', None,
                     'File to write/read public encryption key to/from')
gflags.DEFINE_string('private_keyfile', None,
                     'File to write/read private encryption key to/from')
gflags.DEFINE_integer('key_bits', None,
                      'Size (in bits) of new RSA key')
gflags.DEFINE_string('set', None,
                     'Set name to publish to')
gflags.DEFINE_string('source_set', None,
                     'Set name to publish from; defaults to target set')
gflags.DEFINE_boolean('incremental', True,
                      'Write only differences to existing tables, instead of '
                      'clearing tables and starting from scratch')
gflags.DEFINE_multistring('comment_name', [], 'Comment name to retrieve.')


_COMMANDS = {'decrypt-hash':     utils.DecryptHashInteractive,
             'encrypt-hash':     utils.EncryptHashInteractive,
             'encrypt-password': utils.EncryptPasswordInteractive,
             'fetch-comments':   utils.FetchCommentsInteractive,
             'generate-key':     utils.GenerateKey,
             'generate-password':utils.GeneratePasswordInteractive,
             'hash-password':    utils.HashPasswordInteractive,
             'copy':             use.Copy,
             'dump':             use.PermissionsFile.Dump,
             'list-sets':        use.PermissionsFile.PrintSets,
             'push':             use.PermissionsFile.Push,
             'publish':          use.PermissionsFile.Publish,
             'test':             use.PermissionsFile.Test}


def CheckArgs(func, args):
  argset = set(args)
  spec = inspect.getargspec(func)
  funcargs = set(spec.args)
  if spec.defaults:
    requiredargs = set(spec.args[:-len(spec.defaults)])
  else:
    requiredargs = set(spec.args)

  missing = requiredargs.difference(argset)
  if missing:
    raise app.UsageError('Missing arguments: %s' % ', '.join(missing))

  for extra in argset.difference(funcargs):
    del args[extra]


def main(argv):
  if len(argv) != 2:
    raise app.UsageError('Incorrect arguments')

  args = {}

  if FLAGS.private_keyfile:
    args['private_keyfile'] = FLAGS.private_keyfile

  if FLAGS.file:
    args['self'] = use.PermissionsFile(
        None, private_keyfile=FLAGS.private_keyfile)
    args.pop('private_keyfile', None)  # Not otherwise used
    for file in FLAGS.file:
      args['self'].Parse(open(file, 'r').read())

  if FLAGS.db:
    args['dbh'] = db.Connect(FLAGS.db)

  if FLAGS.set:
    args['dest_set_name'] = FLAGS.set
    args['set_name'] = FLAGS.set

  if FLAGS.source_set:
    args['set_name'] = FLAGS.source_set

  if FLAGS.push_duration:
    args['push_duration'] = FLAGS.push_duration

  if FLAGS.public_keyfile:
    args['public_keyfile'] = FLAGS.public_keyfile

  if FLAGS.comment_name:
    args['comment_names'] = FLAGS.comment_name

  args['incremental'] = FLAGS.incremental

  CheckArgs(_COMMANDS[argv[1]], args)
  try:
    if 'self' in args:
      obj = args.pop('self')
      _COMMANDS[argv[1]](obj, **args)
    else:
      _COMMANDS[argv[1]](**args)
  except db.Error as e:
    # Lose the stack trace; it's not useful for DB errors
    print e
    return 1
  finally:
    if 'dbh' in args:
      args['dbh'].Close()


if __name__ == '__main__':
  app.run()
