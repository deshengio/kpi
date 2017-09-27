-- Table: kpicache

-- DROP TABLE kpicache;

CREATE TABLE kpicache
(
  --id SERIAL Primary Key,
  selector text NOT NULL,
  selectorcontent text NOT NULL,
  closed boolean NOT NULL DEFAULT FALSE,
    AVERAGE real,
	CYCLECOUNT integer,
	PRODUCTCOUNT integer,
	THROUGHPUT real,
			AVAILABILITY real,
			CAPACITYPOTENTIAL real,
			EFFICIENCY real,
			STARTTIME timestamp with time zone NOT NULL,
			ENDTIME timestamp with time zone NOT NULL,
			MACHINEID text NOT NULL,
    unique(selector, selectorcontent,MACHINEID)
);
ALTER TABLE kpicache
  OWNER TO kpiadmin;
