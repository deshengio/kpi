CREATE TABLE cognipro_vr8600_e_sensordata_v2 (
	machine_id varchar(255) NULL,
	created_timestamp timestamp NOT NULL,
	shift varchar(255) NULL,
	plc_timestamp timestamp NOT NULL,
	param_platennumber float8 NULL,
	param_chambernumber float8 NULL,
	param_vac float8 NULL,
	param_sealcurrent float8 NULL,
	param_sealvoltage float8 NULL,
	param_speedcpm float8 NULL,
	param_lifetimecycles float8 NULL,
	param_productcount float8 NULL,
	param_productpresent float8 NULL,
	state_all float8 NULL,
	alarm_1 numeric(10) NULL,
	alarm_2 numeric(10) NULL,
	alarm_3 numeric(10) NULL,
	alarm_4 numeric(10) NULL,
	id_pk numeric(10) NOT NULL,
	param_speedppm float8 NULL,
	CONSTRAINT cognipro_vr8600_e_sensordata_v2_pkey PRIMARY KEY (id_pk)
)
WITH (
	OIDS=FALSE
) ;
CREATE INDEX cognipro_vr8600_e_sensordata_v2_machine_id_idx ON cognipro_vr8600_e_sensordata_v2 (machine_id text_ops) ;
CREATE INDEX cognipro_vr8600_e_sensordata_v2_plc_timestamp_idx ON cognipro_vr8600_e_sensordata_v2 (plc_timestamp timestamp_ops) ;
