#!/usr/bin/python2.4
#
# Copyright 2006 Google Inc. All Rights Reserved.

from distutils.core import setup

setup(name="google-mysql-tools",
      description="Google MySQL Tools",
      url="http://code.google.com/p/google-mysql-tools",
      version="0.1",
      packages=["gmt"],
      scripts=["mypgrep.py", "compact_innodb.py"])
