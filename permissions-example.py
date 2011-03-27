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

__author__ = 'flamingcow@google.com (Ian Gulliver)'

base_tmpl = (Account()
             .GrantPrivileges(database='test', privileges=SELECT | INSERT | UPDATE | DELETE | CREATE | DROP)
             .AddAllowedHost(hostname_pattern='%'))

(base_tmpl.Clone(username='root',
                 # password='foo',
                 password_hash='*F3A2A51A9B0F2BE2468926B4132313728C250DBF')
 .GrantPrivileges(privileges=ALL_PRIVILEGES)
 .Export(set_name='primary')
 .Export(set_name='replica'))
