DROP FUNCTION calculate_availability(character varying,character varying,character varying,integer);

CREATE OR REPLACE FUNCTION calculate_availability(value_stream_name character varying, machine_id character varying, end_time_string character varying, duration_in_sec Integer)
 RETURNS real
 LANGUAGE plpgsql
 STABLE
AS $$
-- value_stream_name: this is the associated value stream on Thing general info page.
-- machine_id: this is the Thing name in Thingworx
-- end_time_string: the time to check in string with format: YYYYMMDDHH24MISS, for example:20170919235959. assume UTC timezone
-- duration_in_sec: the time to look back in second. if you want to look up for 1 minute result, just give 60. 3600 for 1 hour. and 24*3600 for 1 day.

declare
	var_end_time timestamp with time zone;
	var_start_time timestamp with time zone;
	var_last_time timestamp with time zone;
	var_last_value character varying;
	rec RECORD;
	prerec RECORD;
	available_time_in_sec	real;
begin
	IF duration_in_sec <=0 then
		RETURN 0.0;
	END IF;
	-- raise notice 'Start from:%', now();
	select into var_end_time (to_timestamp(end_time_string,'YYYYMMDDHH24MISS')::timestamp with time zone) ;
	select into var_start_time (var_end_time - duration_in_sec * interval '1 second');
	-- raise notice 'end time:%', var_end_time;
	-- raise notice 'start time:%', var_start_time;
	-- get last result right before start time.
	var_last_value := '1'; -- setup default value.
	for prerec in
		select time, property_value 
		from value_stream 
		where entity_id=value_stream_name
		and source_id=machine_id
		and property_name='Status'
		and time <= var_start_time
		order by time desc
		limit 1
	LOOP
		var_last_value := prerec.property_value;
	END LOOP;
	var_last_time := var_start_time;	-- setup start time in loop.
	
	available_time_in_sec = 0.0;
	for rec in 
		select time, property_value 
		from value_stream 
		where entity_id=value_stream_name
		and source_id=machine_id
		and property_name='Status'
		and time <= var_end_time
		and time > var_start_time
		order by time
	LOOP
		-- RAISE NOTICE '%:% - %:%', var_last_time, var_last_value, rec.time, rec.property_value;
		if var_last_value = '1' then
			available_time_in_sec := extract(epoch from rec.time - var_last_time)+ available_time_in_sec;
		ELSEIF var_last_value = '2' then
			available_time_in_sec := extract(epoch from rec.time - var_last_time)+ available_time_in_sec;
		ELSEIF var_last_value = '3' then
			available_time_in_sec := extract(epoch from rec.time - var_last_time)+ available_time_in_sec;
		END IF;
		var_last_time := rec.time;
		var_last_value := rec.property_value;
	END LOOP;
	-- add tail time.
	if var_last_value = '1' then
		available_time_in_sec := extract(epoch from var_end_time - var_last_time)+ available_time_in_sec;
	ELSEIF var_last_value = '2' then
		available_time_in_sec := extract(epoch from var_end_time - var_last_time)+ available_time_in_sec;
	ELSEIF var_last_value = '3' then
		available_time_in_sec := extract(epoch from var_end_time - var_last_time)+ available_time_in_sec;
	END IF;
	-- raise notice 'End at:%', now();
	RETURN available_time_in_sec/duration_in_sec;
end;

$$
