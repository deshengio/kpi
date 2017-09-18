from pprint import pprint
import os,sys
import pg8000 as dbapi
import argparse
#from multiprocessing import Process
import threading
from datetime import datetime
import time

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

try:
    from . import configurator
    from . import db
    from . import normalizer
    from . import thingmodules
except ImportError:
    import configurator
    import db
    import normalizer
    import thingmodules

def singleProcess(thread):
    '''
    one process to handle one thread
    :param kpiconfig:
    :param dbManager:
    :return:
    '''
    starttime = datetime.now()
    print("Thread:{}, start at:{}".format(thread.ThreadName, starttime))

    required_tables = normalizer.queryTableListFromThread(thread)
    if thread.VerifyTableStructure:
        missed_tables = normalizer.verifyTargetTables(thread,
                                                      required_tables)
        if len(missed_tables) >0:
            if thread.AutoCreateTable:
                normalizer.buildRequiredTables(thread,
                                               missed_tables
                                               )
            else:
                print("Required Table doesn't exist:{}".format(missed_tables))
                return
        else:
            print("Table verified:{}\n".format(required_tables))

    for thing in thread.Things:
        if normalizer.isNewThing(thread, thing):
            insert_sql,datapool_sql = normalizer.initThingFirstRow(thread,thing)
            if not insert_sql:
                # no data presented in value stream yet.
                print("no data yet for this thing.")
            else:
                curr = thread.TargetDB.getDBConnection().cursor()
                curr.execute(insert_sql)
                curr.execute(datapool_sql)
                thread.TargetDB.getDBConnection().targetConnection.commit()
                curr.close()
        else:
            #print("it's not new:")
            total_values,value_types = normalizer.normalizeThingRecords(thread, thing)
            if total_values:
                #pprint(total_values)
                normalizer.pushIncreamentalDataToDb(thread, thing,total_values,value_types)
                    #print("Finished")
                    #dbManager.targetConnection.commit()
                    #curr.close()
    endtime = datetime.now()
    print("Thread:{}, end at:{}, times:{}".format(thread.ThreadName, endtime, endtime-starttime))

def startProcess(kpiconfig):
    processes = []
    index=1
    for thread in kpiconfig.threads:
        index += 1
        proc = threading.Thread(target=singleProcess,name=thread.ThreadName,
                                args=(thread,))
        processes.append(proc)
        proc.start()

    for proc in processes:
        proc.join()

def loopProcess(kpiconfig):
    try:
        while True:
            starttime = time.time()
            print("Start a new round at:{}\n".format(starttime))
            startProcess(kpiconfig)
            endtime = time.time()
            timecost = int(endtime-starttime)
            print("\nEnd at at:{}, spent {} seconds".format(endtime, timecost))
            remaining = kpiconfig.intervalCheckTime-timecost
            if remaining > 5:
                print("\n\n.....go to sleep for {} seconds....\n\n".format(remaining))
                time.sleep(remaining)
            else:
                print("\n\n.....remaining time is less than 5 seconds, let's kick off next round.")
                time.sleep(2)

    except KeyboardInterrupt:
        print('Stopped')

if __name__ == '__main__':
    kpiconfig = configurator.KpiConfiguration(os.path.join(SCRIPT_DIR,"../config/config.json"))
    #startProcess(kpiconfig)
    loopProcess(kpiconfig)
