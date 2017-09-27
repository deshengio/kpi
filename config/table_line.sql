-- Table: line

--DROP TABLE line;

CREATE TABLE line
(
  --id SERIAL Primary Key,
  name text NOT NULL Primary Key,
  description text,
  createtime timestamp with time zone NOT NULL default (now() at time zone 'utc'),
  lastmodifytime timestamp with time zone NOT NULL default (now() at time zone 'utc'),
  isvalid boolean NOT NULL DEFAULT TRUE,
  shift1start text NOT null,
  shift1duration text NOT null,
  shift2start text NOT null,
  shift2duration text NOT NULL,
  shift3start text NOT NULL,
  shift3duration text NOT NULL,
  timezoneoffset  integer NOT NULL

);
ALTER TABLE line
  OWNER TO kpiadmin;
