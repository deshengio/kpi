-- Table: steamsensor

-- DROP TABLE steamsensor;

CREATE TABLE steamsensor
(
  id SERIAL Primary Key,
  ThingName text NOT NULL,
  TotalFlow real NOT NULL,
  Temperature real,
  Pressure real,
  lasttime timestamp with time zone NOT NULL,
  lastid integer UNIQUE
);
ALTER TABLE steamsensor
  OWNER TO kpiadmin;
