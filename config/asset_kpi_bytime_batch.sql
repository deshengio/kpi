DROP FUNCTION asset_kpi_bytime_batch(character varying,character varying[],character varying,character varying, integer);

CREATE OR REPLACE FUNCTION asset_kpi_bytime_batch(value_stream_name character varying, machine_ids character varying[],start_time_string character varying, end_time_string character varying, idea_run_rate integer)
  RETURNS TABLE(AVERAGE real, CYCLECOUNT integer, PRODUCTCOUNT integer, THROUGHPUT real, AVAILABILITY real, CAPACITYPOTENTIAL real, EFFICIENCY real, STARTTIME character varying, ENDTIME character varying, MACHINEID character varying )
   LANGUAGE plpgsql
 STABLE
   AS
$$
declare
	myindex integer;
	machine_id character varying;
	one_machine RECORD;
	myAVERAGE real;
	myCYCLECOUNT integer;
	myPRODUCTCOUNT integer;
	myTHROUGHPUT  real;
	myAVAILABILITY  real;
	myCAPACITYPOTENTIAL  real;
	myEFFICIENCY  real;
	mySTARTTIME  character varying;
	myENDTIME  character varying;
	myMACHINEID  character varying;
BEGIN
	--RAISE NOTICE 'INPUT ARRAY IS:%', machine_ids;
	
	myindex := 0;
	foreach machine_id in 
		array machine_ids 
	LOOP
		--RAISE NOTICE 'CURRENT MACHINE IS:%', machine_id;

		for myAVERAGE,myCYCLECOUNT,myPRODUCTCOUNT,myTHROUGHPUT,myAVAILABILITY,myCAPACITYPOTENTIAL,myEFFICIENCY,mySTARTTIME,myENDTIME,myMACHINEID in
			select onemachine.AVERAGE, onemachine.CYCLECOUNT, onemachine.PRODUCTCOUNT,onemachine.THROUGHPUT,onemachine.AVAILABILITY, 
			onemachine.CAPACITYPOTENTIAL,onemachine.EFFICIENCY, onemachine.STARTTIME,onemachine.ENDTIME, onemachine.MACHINEID
			from asset_kpi_bytime(value_stream_name,machine_id,start_time_string,end_time_string,idea_run_rate) onemachine
		LOOP

			AVERAGE := myAVERAGE;
			CYCLECOUNT := myCYCLECOUNT;
			PRODUCTCOUNT := myPRODUCTCOUNT;
			THROUGHPUT := myTHROUGHPUT;
			AVAILABILITY := myAVAILABILITY;
			CAPACITYPOTENTIAL := myCAPACITYPOTENTIAL;
			EFFICIENCY := myEFFICIENCY;
			STARTTIME := mySTARTTIME;
			ENDTIME := myENDTIME;
			MACHINEID := myMACHINEID;
			
			RETURN NEXT;
		END LOOP;
	END LOOP;

	RETURN;

END;
    $$
