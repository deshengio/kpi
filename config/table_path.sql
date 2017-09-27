-- Table: path

--DROP TABLE path;

CREATE TABLE path
(
  --id SERIAL Primary Key,
  name text NOT NULL Primary Key,
  linename text REFERENCES line (name),
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
ALTER TABLE path
  OWNER TO kpiadmin;
