#!/usr/bin/python2.2
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

"""A simple module to run commands in parallel.

This module provides two classes, CommandPool and CommandResult.  A
cmomand pool is given a maximum number of processes and a list of
commands to run, and executes those processes in parallel.  It saves
the output and error codes, failing the entire job and attempting to
clean up if any job fails.
"""

__author__ = 'chip@google.com (Chip Turner)'

import os
import subprocess
import tempfile
import time

class CommandResult(object):
  """A simple class to represent the result of a command.

  A simple data-holding class.

  Member fields:
    command: the command executed
    data: user-specified data associated with this command
    output: filehandle to the stdout+stderr output of the command
    returncode: the return code of the program
  """
  def __init__(self, command, data, output, returncode):
    self.command = command
    self.data = data
    self.output = output
    self.returncode = returncode


class CommandPool(object):
  """A class to run multiple commands in parallel.

  This class allows for multiple commands to be executed in parallel,
  up to max_size at once.  The output of the commands and return codes
  are available through accessors after run() has been called.
  """
  def __init__(self, max_size):
    """Construct a command pool that runs up to max_size parallel commands.

    Args:
      max_size: maximum number of parallel processes to execute at once.

    Members fields:
      failures: list of CommandResults for failed tasks
      successes: list of CommandResults for successful tasks
    """
    self.__max_size = max_size
    self.__todo = []
    self.__pidmap = {}
    self.failures = []
    self.successes = []

  def submit(self, command, data=None):
    """Add a command and user-specified data to the queue to execute.

    Args:
      command: command to execute
      data: user-specified data associated with this command
    """
    self.__todo.append((command, data))

  def run(self):
    """Execute the commands in the pool.

    When this method returns, successful jobs are in the object's
    'successes' instance variable, and failures are in the 'failures'
    instance variable.

    Args:
      None

    Returns:
      boolean indicating success or failure
    """
    running_jobs = []
    # logic: while we are running any jobs or have any left...
    while running_jobs or self.__todo:
      # ... spawn more until we're out of jobs or at max_size...
      while self.__todo and len(running_jobs) < self.__max_size:
        command, data = self.__todo.pop(0)
        child, output = self.__make_job(command)
        # ... tracking the child/job mapping via __pidmap...
        self.__pidmap[child.pid] = (command, data, output)
        running_jobs.append(child)

      abort = 0
      still_running = []
      # ... now poll for completion, constructing still_running...
      for child in running_jobs:
        if child.poll() is None:
          still_running.append(child)
        else:
          # ... task completed, check for failure or success...
          command, data, output = self.__pidmap[child.pid]
          output.seek(0)
          if child.returncode != 0:
            # ... a single failure, for now, aborts our pool...
            abort = 1
            failure = CommandResult(command, data, output, child.returncode)
            self.failures = [ failure ]
          else:
            self.successes.append(CommandResult(command, data, output, 0))
      running_jobs = still_running

      # ... if we need to abort, kill our children in a very crude way

      # XXX: if subprocess supported program groups, this would be
      # much easier and effective.
      if abort:
        for child in running_jobs:
          os.kill(child.pid, 15)
        time.sleep(1)
        for child in running_jobs:
          os.kill(child.pid, 9)
        return 0

      time.sleep(0.1)

    return 1

  def __make_job(self, command):
    """Private helper to create a job with redirected stdout and stderr.

    Returns:
      (child, tmpfh) tuple of the subprocess object and filehandle to the output
    """
    tmpfh = tempfile.TemporaryFile()
    child = subprocess.Popen(command, shell=True,
                             stdout=tmpfh, stderr=subprocess.STDOUT)
    return child, tmpfh
