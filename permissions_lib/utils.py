#!/usr/bin/python2.6
#
# Copyright 2011 Google Inc.
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

"""Utilities for working with MySQL permissions."""

__author__ = 'flamingcow@google.com (Ian Gulliver)'

import base64
import getpass
import hashlib
import os
import random
import string
import sys

try:
  from tlslite.utils import compat
  from tlslite.utils import keyfactory
except (ValueError, ImportError):
  from tlslite.tlslite.utils import compat
  from tlslite.tlslite.utils import keyfactory


def GeneratePassword(passwd_length=12):
  """Return a password of the appropriate length (default 12)."""
  chars = list(set(string.printable) - set(string.whitespace))
  password = ""
  for _ in xrange(passwd_length):
    password += random.choice(chars)
  return password


def GeneratePasswordInteractive():
  print GeneratePassword()


def HashPassword(plaintext):
  """Generate a MySQL password hash from a string."""
  hashobj = hashlib.sha1(hashlib.sha1(plaintext).digest())
  return '*%s' % hashobj.hexdigest().upper()


def HashPasswordInteractive():
  """Generate a MySQL password hash with interaction from the console."""
  pass1 = getpass.getpass('Password: ')
  pass2 = getpass.getpass('Password (again): ')
  assert pass1 == pass2, 'Passwords do not match'
  print HashPassword(pass1)


def GenerateKey(public_keyfile, private_keyfile, key_bits=2048):
  """Generate an RSA key pair."""
  public = open(public_keyfile, 'w')
  os.fchmod(public.fileno(), 0400)
  private = open(private_keyfile, 'w')
  os.fchmod(private.fileno(), 0400)
  key = keyfactory.generateRSAKey(key_bits)
  public.write(key.writeXMLPublicKey() + '\n')
  private.write(key.write() + '\n')


def PrivateKeyFromFile(private_keyfile):
  """Load a private RSA key from a file."""
  xml = open(private_keyfile, 'r').read()
  return keyfactory.parseXMLKey(xml, private=True)


def PublicKeyFromFile(public_keyfile):
  """Load a public RSA key from a file."""
  xml = open(public_keyfile, 'r').read()
  return keyfactory.parseXMLKey(xml, public=True)


def EncryptHash(key, hashed):
  """Encrypt a hash with a public key."""
  encrypted = key.encrypt(compat.stringToBytes(hashed))
  return base64.b64encode(encrypted)


def EncryptHashInteractive(public_keyfile):
  """Encrypt a hash from the console."""
  key = PublicKeyFromFile(public_keyfile)
  mysql_hash = raw_input('MySQL hash: ')
  # Try to make it hard for confused people to botch their permissions file.
  if not (mysql_hash.startswith('*') and len(mysql_hash) == 41):
    print 'BEWARE! That doesn\'t look like a new-style mysql hash!'
  print EncryptHash(key, mysql_hash)


def EncryptPasswordInteractive(public_keyfile):
  """Hash and encrypt a password with interaction from the console."""
  key = PublicKeyFromFile(public_keyfile)
  pass1 = getpass.getpass('Password: ')
  pass2 = getpass.getpass('Password (again): ')
  assert pass1 == pass2, 'Passwords do not match'
  hashed = HashPassword(pass1)
  print EncryptHash(key, hashed)


def TestEncryptedHash(ciphertext):
  """Test an encrypted hash as much as possible without the key."""
  try:
    base64.b64decode(ciphertext)
    return True
  except TypeError:
    return False


def DecryptHash(key, ciphertext):
  """Decrypt a hash with a private key."""
  encrypted = base64.b64decode(ciphertext)
  plaintext = key.decrypt(compat.stringToBytes(encrypted))
  if plaintext:
    return compat.bytesToString(plaintext)
  else:
    # decryption failed
    return None


def DecryptHashInteractive(private_keyfile):
  """Decrypt a hash with interaction from the console."""
  key = PrivateKeyFromFile(private_keyfile)
  encrypted = raw_input('Encrypted string: ')
  print DecryptHash(key, encrypted)


def FetchComments(permissions_file, set_name, comment_names, usernames):
  """Fetch a tuple of (username, comment values) for each username."""
  set_obj = permissions_file.GetSet(set_name)
  for username in usernames:
    yield (username, set_obj.GetComments(comment_names, username))


def FetchCommentsInteractive(self, set_name, comment_names):
  """Read usernames from stdin and print a list of comment values."""
  usernames = (line.strip() for line in sys.stdin)
  comment_values = FetchComments(self, set_name, comment_names, usernames)
  for username, values in comment_values:
    print '%s\t%s' % (username, '\t'.join(values))
