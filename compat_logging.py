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

import logging
import sys

logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)-8s %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

from logging import debug, info, error

def fatal(*args, **kwargs):
  logging.fatal(*args, **kwargs)
  sys.exit(1)
