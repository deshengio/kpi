-- Table: datapool

-- DROP TABLE datapool;

CREATE TABLE datapool
(
  id SERIAL Primary Key,
  ThingName text NOT NULL,
  ValueStreamName text NOT NULL,
  KeyPropertyName text NOT NULL,
  lastid integer UNIQUE,
  lasttime timestamp with time zone NOT NULL,
  closed boolean Default false
);
ALTER TABLE datapool
  OWNER TO kpiadmin;
