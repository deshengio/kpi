import os
import sys
import datetime
import pg8000 as dbapi
import pytz

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))


try:
    from . import configurator
    from . import db
    from . import thingmodules
except ImportError:
    import configurator
    import db
    import thingmodules


def calculate_availability(thread, thing, value_stream_name,machine_id,end_time_str,duration_in_sec):
    '''
    simulate calculate_availability stored procedure and compare performance.
    :param value_stream_name:
    :param machine_id:
    :param end_time:
    :param duration_in_sec:
    :return:
    '''
    if duration_in_sec <= 0.0:
        return 0.0

    end_time = pytz.utc.localize(datetime.datetime.strptime(end_time_str,'%Y%m%d%H%M%S'))
    start_time = end_time - datetime.timedelta(seconds=duration_in_sec)
    start_time_str = start_time.strftime('%Y%m%d%H%M%S')

    query_last = "select time, property_value from value_stream"
    query_last += "\nwhere entity_id='{}'".format(value_stream_name)
    query_last += "\nand source_id='{}'".format(machine_id)
    query_last += "\nand property_name='Status'"
    query_last += "\nand time<=(to_timestamp('{}','YYYYMMDDHH24MISS')::timestamp with time zone)".format(start_time_str)
    query_last += "\norder by time desc limit 1;"

    var_last_value = "1"  # default value

    curr = thread.SourceDB.getDBConnection().cursor()
    curr.execute(query_last)
    for row in curr.fetchall():
        var_last_value = row[1]

    var_last_time = start_time

    query_full = "select time, property_value from value_stream"
    query_full += "\nwhere entity_id='{}'".format(value_stream_name)
    query_full += "\nand source_id='{}'".format(machine_id)
    query_full += "\nand property_name='Status'"
    query_full += "\nand time>(to_timestamp('{}','YYYYMMDDHH24MISS')::timestamp with time zone)".format(start_time_str)
    query_full += "\nand time<=(to_timestamp('{}','YYYYMMDDHH24MISS')::timestamp with time zone)".format(end_time_str)
    query_full += "\norder by time;"

    curr.execute(query_full)
    total_time_in_sec = datetime.timedelta(seconds=0)
    for row in curr.fetchall():
        if var_last_value in ['1','2','3']:
            total_time_in_sec += row[0] - var_last_time

        var_last_time = row[0]
        var_last_value = row[1]

    if var_last_value in ['1', '2', '3']:
        total_time_in_sec += end_time - var_last_time

    #print("total number: {}".format(total_time_in_sec))
    total_time_in_real = total_time_in_sec.seconds + total_time_in_sec.microseconds/1E6
    return total_time_in_real * 1.0 / duration_in_sec

if __name__ == '__main__':
    kpiconfig = configurator.KpiConfiguration(os.path.join(SCRIPT_DIR, "../config/config.json"))


    for thread in kpiconfig.threads:
        if thread.ThreadName == 'SteamSensorTread':

            start_time = datetime.datetime.now()
            start_str = (start_time - datetime.timedelta(seconds=5)).strftime('%Y%m%d%H%M%S')
            print("Start time:{} -> {}".format(start_time, start_str))
            for thing in thread.Things:
                print("result for:{}->{}->\n\t\tMin->{}\n\t\tHour->{};\n\t\tDay->{}".format(
                    thread.ThreadName,
                    thing.ThingName,
                    calculate_availability(thread,thing,thing.ValueStreamName,thing.ThingName,
                                           start_str,60),
                    calculate_availability(thread, thing, thing.ValueStreamName, thing.ThingName,
                                           start_str, 3600),
                    calculate_availability(thread, thing, thing.ValueStreamName, thing.ThingName,
                                           start_str, 86400)
                ))

            end_time = datetime.datetime.now()
            print("End time:{}, duration:{}".format(end_time,end_time - start_time))