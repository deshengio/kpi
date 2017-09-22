CREATE SEQUENCE cognipro_vr8600e_calculatedkpi_byminute_seq;
CREATE TABLE cognipro_vr8600e_calculatedkpi_byminute (
	ref_id varchar(255) NULL,
	machine_id varchar(255) NULL,
	created_timestamp timestamp NULL,
	shift varchar(255) NULL,
	plc_timestamp timestamp NULL,
	kpi_availability float8 NULL,
	kpi_efficiency float8 NULL,
	kpi_throughput float8 NULL,
	id_pk numeric(10) NOT NULL DEFAULT nextval('cognipro_vr8600e_calculatedkpi_byminute_seq'::regclass),
	param_speedcpm float8 NULL,
	param_lifetimecycles float8 NULL,
	param_productcount float8 NULL,
	CONSTRAINT cognipro_vr8600e_calculatedkpi_byminute_pk PRIMARY KEY (id_pk)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX cognipro_vr8600e_calculatedkpi_byminute_created_timestamp_idx ON cognipro_vr8600e_calculatedkpi_byminute (created_timestamp timestamp_ops) ;
CREATE INDEX cognipro_vr8600e_calculatedkpi_byminute_machine_id_idx ON cognipro_vr8600e_calculatedkpi_byminute (machine_id text_ops) ;
