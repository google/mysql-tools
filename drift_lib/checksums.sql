-- Copyright 2011 Google Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE DATABASE IF NOT EXISTS admin;

CREATE TABLE IF NOT EXISTS admin.Checksums (
  DatabaseName varchar(256) NOT NULL,
  TableName varchar(256) NOT NULL,
  Chunk int(11) NOT NULL,
  JobStarted timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ChunkDone timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  Offsets text NOT NULL,
  Checksums text NOT NULL,
  Count int(11) NOT NULL,
  PRIMARY KEY (JobStarted, DatabaseName, TableName, Chunk)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS admin.ChecksumsGolden (
  DatabaseName varchar(256) NOT NULL,
  TableName varchar(256) NOT NULL,
  Chunk int(11) NOT NULL,
  JobStarted timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ChunkDone timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  Offsets text NOT NULL,
  Checksums text NOT NULL,
  Count int(11) NOT NULL,
  PRIMARY KEY (JobStarted, DatabaseName, TableName, Chunk)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS admin.ChecksumLog (
  DatabaseName VARCHAR(256) NOT NULL,
  TableName VARCHAR(256) NOT NULL,
  LastChecksumTime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (DatabaseName, TableName)
) ENGINE = InnoDB;
