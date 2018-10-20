
drop database drcron;
create database drcron;

use drcron;

create table if not exists jobs (
  id bigint unsigned auto_increment not null primary key,
  created DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated DATETIME ON UPDATE CURRENT_TIMESTAMP,
  name varchar(255) NOT NULL, 
  schedule varchar(255) NOT NULL,
  command varchar(8192),
  enabled bool,
  run_parallel tinyint unsigned default 1,
  schedule_error varchar(255),
  /* I don't want to update the schedule error too rapidly.  can consider as part of query and backoff both parsing and recording error */
  schedule_error_time datetime, 
  unique key(name,enabled)
);

INSERT INTO jobs ( name, schedule, command, enabled )  VALUES 
("sleep10job", "* * * * *", "slept=0; while [ $slept -lt 10 ]; do sleep 1; slept=$(($slept+1)); echo slept $slept; done", true ) , 
("sleep5job", "* * * * *", "sleep 5; echo slept 5", true ) 
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
  job_id bigint unsigned not null,
  worker_id bigint unsigned,
  queued datetime DEFAULT CURRENT_TIMESTAMP,
  scheduled_start datetime not null,
  started datetime, 
  ended  datetime,
  output varchar(1024),
  exit_code tinyint unsigned,
  FOREIGN KEY (job_id) REFERENCES jobs (id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  FOREIGN KEY (worker_id) REFERENCES workers (id)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

/* TODO: could also store global settings and state in sql table
  - last schedule run, so we don't run too often?
*/
