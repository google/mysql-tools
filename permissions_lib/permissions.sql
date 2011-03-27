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

CREATE TABLE IF NOT EXISTS mysql.mapped_user (
  User char(16) binary DEFAULT '' NOT NULL,
  Role char(16) binary DEFAULT '' NOT NULL,
  Password char(41) character set latin1 collate latin1_bin DEFAULT '' NOT NULL,
  PasswordChanged Timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
  ssl_type enum('','ANY','X509','SPECIFIED') character set utf8 NOT NULL default '',
  ssl_cipher blob NOT NULL,
  x509_issuer blob NOT NULL,
  x509_subject blob NOT NULL,
  PRIMARY KEY (User, Role, Password)
) ENGINE=MyISAM
  CHARACTER SET utf8 COLLATE utf8_bin
  COMMENT='Mapped users';

CREATE DATABASE IF NOT EXISTS admin;

DROP TABLE IF EXISTS admin.MysqlPermissionSets;
CREATE TABLE admin.MysqlPermissionSets (
  MysqlPermissionSetId int(11) NOT NULL default '0',
  PermissionSetLabel varchar(60) NOT NULL default '',
  LastUpdated timestamp NOT NULL default CURRENT_TIMESTAMP,
  PushDuration int(11) NOT NULL default '1800',
  PRIMARY KEY  (MysqlPermissionSetId),
  UNIQUE KEY PermissionSetLabel (PermissionSetLabel)
);
INSERT INTO admin.MysqlPermissionSets (MysqlPermissionSetId, PermissionSetLabel) VALUES
  (1, 'primary'),
  (2, 'replica');

DROP TABLE IF EXISTS admin.MysqlPermissionsUser;
CREATE TABLE admin.MysqlPermissionsUser LIKE mysql.user;
ALTER TABLE admin.MysqlPermissionsUser
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, Host, User),
  ENGINE=InnoDB;

DROP TABLE IF EXISTS admin.MysqlPermissionsMappedUser;
CREATE TABLE admin.MysqlPermissionsMappedUser LIKE mysql.mapped_user;
ALTER TABLE admin.MysqlPermissionsMappedUser
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, User, Password),
  ENGINE=InnoDB;

DROP TABLE IF EXISTS admin.MysqlPermissionsHost;
CREATE TABLE admin.MysqlPermissionsHost LIKE mysql.host;
ALTER TABLE admin.MysqlPermissionsHost
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, Host, Db),
  ENGINE=InnoDB;

DROP TABLE IF EXISTS admin.MysqlPermissionsDb;
CREATE TABLE admin.MysqlPermissionsDb LIKE mysql.db;
ALTER TABLE admin.MysqlPermissionsDb
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, Host, Db, User),
  ENGINE=InnoDB;

DROP TABLE IF EXISTS admin.MysqlPermissionsTablesPriv;
CREATE TABLE admin.MysqlPermissionsTablesPriv LIKE mysql.tables_priv;
ALTER TABLE admin.MysqlPermissionsTablesPriv
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, Host, Db, User, Table_name),
  ENGINE=InnoDB;

DROP TABLE IF EXISTS admin.MysqlPermissionsColumnsPriv;
CREATE TABLE admin.MysqlPermissionsColumnsPriv LIKE mysql.columns_priv;
ALTER TABLE admin.MysqlPermissionsColumnsPriv
  ADD COLUMN MysqlPermissionSetId int(11) NOT NULL default 0 FIRST,
  DROP PRIMARY KEY,
  ADD PRIMARY KEY (MysqlPermissionSetId, Host, Db, User, Table_name, Column_name),
  ENGINE=InnoDB;

CREATE DATABASE IF NOT EXISTS adminlocal;

DROP TABLE IF EXISTS adminlocal.LocalMysqlPermissionState;
CREATE TABLE adminlocal.LocalMysqlPermissionState (
  MysqlPermissionSetId int(11) NOT NULL default 0,
  LastPushed timestamp NOT NULL default CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (MysqlPermissionSetId)
) ENGINE=InnoDB;
