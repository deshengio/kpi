-- Table: selectorcontent

DROP TABLE selectorcontent;

CREATE TABLE selectorcontent
(
    id SERIAL Primary Key,
    selector text REFERENCES selector (selector),
  selectorcontent text NOT NULL,
  description text,
  unique(selector, selectorcontent)
);
ALTER TABLE selectorcontent
  OWNER TO kpiadmin;
