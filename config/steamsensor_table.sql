-- Table: steamsensor

-- DROP TABLE steamsensor;

CREATE TABLE steamsensor
(
  id SERIAL Primary Key,
  ThingName text NOT NULL,
  TotalFlow real NOT NULL,
  Temperature real,
  Pressure real,
  time timestamp with time zone NOT NULL
);
ALTER TABLE steamsensor
  OWNER TO kpiadmin;
