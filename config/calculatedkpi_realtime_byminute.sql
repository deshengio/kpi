CREATE SEQUENCE cognipro_vr8600e_calculatedkpi_realtime_byminute_seq;
CREATE TABLE cognipro_vr8600e_calculatedkpi_realtime_byminute (
	id_pk numeric(10) NOT NULL DEFAULT nextval('cognipro_vr8600e_calculatedkpi_realtime_byminute_seq'::regclass),
	ref_id varchar(255) NULL,
	machine_id varchar(255) NULL,
	created_timestamp timestamp NULL,
	shift varchar(255) NULL,
	plc_timestamp timestamp NULL,
	kpi_availability float8 NULL DEFAULT 0,
	kpi_capacitypotential float8 NULL DEFAULT 0,
	kpi_efficiency float8 NULL DEFAULT 0,
	kpi_throughput float8 NULL DEFAULT 0,
	kpi_alarmtime_01 float8 NULL DEFAULT 0,
	kpi_alarmtime_02 float8 NULL DEFAULT 0,
	kpi_alarmtime_03 float8 NULL DEFAULT 0,
	kpi_alarmtime_04 float8 NULL DEFAULT 0,
	kpi_alarmtime_05 float8 NULL DEFAULT 0,
	kpi_alarmtime_06 float8 NULL DEFAULT 0,
	kpi_alarmtime_07 float8 NULL DEFAULT 0,
	kpi_alarmtime_08 float8 NULL DEFAULT 0,
	kpi_alarmtime_09 float8 NULL DEFAULT 0,
	kpi_alarmtime_10 float8 NULL DEFAULT 0,
	kpi_alarmtime_11 float8 NULL DEFAULT 0,
	kpi_alarmtime_12 float8 NULL DEFAULT 0,
	kpi_alarmtime_13 float8 NULL DEFAULT 0,
	kpi_alarmtime_14 float8 NULL DEFAULT 0,
	kpi_alarmtime_15 float8 NULL DEFAULT 0,
	kpi_alarmtime_16 float8 NULL DEFAULT 0,
	kpi_alarmtime_17 float8 NULL DEFAULT 0,
	kpi_alarmtime_18 float8 NULL DEFAULT 0,
	kpi_alarmtime_19 float8 NULL DEFAULT 0,
	kpi_alarmtime_20 float8 NULL DEFAULT 0,
	kpi_alarmtime_21 float8 NULL DEFAULT 0,
	kpi_alarmtime_22 float8 NULL DEFAULT 0,
	kpi_alarmtime_23 float8 NULL DEFAULT 0,
	kpi_alarmtime_24 float8 NULL DEFAULT 0,
	kpi_alarmtime_25 float8 NULL DEFAULT 0,
	kpi_alarmtime_26 float8 NULL DEFAULT 0,
	kpi_alarmtime_27 float8 NULL DEFAULT 0,
	kpi_alarmtime_28 float8 NULL DEFAULT 0,
	kpi_alarmtime_29 float8 NULL DEFAULT 0,
	kpi_alarmtime_30 float8 NULL DEFAULT 0,
	kpi_statetime_01 float8 NULL DEFAULT 0,
	kpi_statetime_02 float8 NULL DEFAULT 0,
	kpi_statetime_03 float8 NULL DEFAULT 0,
	kpi_statetime_04 float8 NULL DEFAULT 0,
	kpi_statetime_08 float8 NULL DEFAULT 0,
	kpi_statetime_32 float8 NULL DEFAULT 0,
	kpi_alarmtime_31 float8 NULL DEFAULT 0,
	kpi_alarmtime_32 float8 NULL DEFAULT 0,
	kpi_statetime_05 float8 NULL DEFAULT 0,
	kpi_alarmtime_33 float8 NULL DEFAULT 0,
	kpi_alarmtime_34 float8 NULL DEFAULT 0,
	kpi_alarmtime_35 float8 NULL DEFAULT 0,
	kpi_alarmtime_36 float8 NULL DEFAULT 0,
	kpi_alarmtime_37 float8 NULL DEFAULT 0,
	CONSTRAINT cognipro_vr8600e_calculatedkpi_realtime_byminute_pk PRIMARY KEY (id_pk)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX cognipro_vr8600e_calculatedkpi_realtime_byminute_machine_id_idx ON cognipro_vr8600e_calculatedkpi_realtime_byminute (machine_id text_ops) ;
CREATE INDEX cognipro_vr8600e_calculatedkpi_realtime_byminute_plc_timestamp_ ON cognipro_vr8600e_calculatedkpi_realtime_byminute (plc_timestamp timestamp_ops) ;
