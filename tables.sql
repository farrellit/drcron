
create database if not exists drcron;

use drcron;

create table if not exists jobs (
  id bigint unsigned auto_increment not null primary key,
  name varchar(255) not null, 
  schedule varchar(255),
  command varchar(8192),
  enabled bool,
  run_parallel tinyint unsigned default 1,
  unique key(name,enabled)
);

INSERT INTO jobs ( name, schedule, command, enabled )  VALUES 
("sleep10job", "* * * * *", "bash -c 'sleep 10; echo slept 10'", true ) ,
("sleep5job", "* * * * *", "bash -c 'sleep 5; echo slept 5'", true ) 
ON DUPLICATE KEY UPDATE 
  command=VALUES(command),
  schedule=VALUES(schedule)
;

CREATE TABLE if not exists workers (
  id bigint unsigned auto_increment not null primary key,
  info varchar(1024),
  added datetime,
  heartbeat datetime,
  exited datetime
);

create table if not exists job_executions (
  id bigint unsigned auto_increment not null primary key,
  job_id bigint not null,
  worker_id bigint not null,
  started datetime, 
  ended  datetime,
  output varchar(1024),
  exit_code tinyint unsigned
);

