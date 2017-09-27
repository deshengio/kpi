-- Table: selector

DROP TABLE selector;

CREATE TABLE selector
(
  selector text NOT NULL,
  description text,
  unique(selector)
);
ALTER TABLE selector
  OWNER TO kpiadmin;
