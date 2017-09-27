-- Table: pathdevices

DROP TABLE pathdevices;

CREATE TABLE pathdevices
(
  id SERIAL Primary Key,
  devicename text NOT NULL,
  pathname text REFERENCES path (name),
  note text,
  createtime timestamp with time zone NOT NULL default (now() at time zone 'utc'),
  lastmodifytime timestamp with time zone NOT NULL default (now() at time zone 'utc'),
  isvalid boolean NOT NULL DEFAULT TRUE,
  unique(devicename,pathname)
);
ALTER TABLE pathdevices
  OWNER TO kpiadmin;
