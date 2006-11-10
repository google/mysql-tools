#!/usr/bin/python2.4
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

"""A utility class to aid in the reading of config files.

Uses the underlying ConfigParser class to read a config file that can
contain database hosts, passwords, etc.
"""

__author__ = 'chip@google.com (Chip Turner)'

import ConfigParser
import os
import re

_config_helper = None
def Init():
  global _config_helper
  _config_helper = ConfigParser.ConfigParser()
  _config_helper.read(["/etc/database_configs",
                       os.path.expanduser("~/.database_configs")])


_csv_re = re.compile(r',\s*')
def ExpandDatabaseAliases(name):
  if (_config_helper.has_section("aliases") and
      _config_helper.has_alias("aliases", name)):
    aliases = _csv_re.split(_config_helper.get("aliases", name))
  else:
    aliases = [ name ]

  expanded_aliases = []
  for alias in aliases:
    if alias.find('#') != -1:
      num_shards = GetGlobal("num_shards")
      for i in range(int(num_shards)):
        expanded_aliases.append(alias.replace("#", str(i)))
    else:
      expanded_aliases.append(alias)

  return expanded_aliases


def GetGlobal(name):
  if _config_helper.has_section("global"):
    return _config_helper.get("global", name)
  else:
    return None
