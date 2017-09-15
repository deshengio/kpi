from pprint import pprint
import os,sys
import pg8000 as dbapi

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import configuration
import db

def initNewThing(sourceConn, targetConn, valueStreamName, thing):
    '''
    create first row of recourd in target db.
    :param sourceConn:
    :param targetConn:
    :param valueStreamName:
    :param thing:
    :return:
    '''

def isNewThing(conn, valueStreamName,thingName,targetTable):
    '''
    check whether this is a new thing to process.
    look up both tables and check whether records exists or not.
    :param conn:
    :param valueStreamName:
    :param thingName:
    :return:
    '''
    query_datapool = "select count(*) from datapool where thingname='{}' and valuestreamname='{}';".format(
        thingName,valueStreamName
    )
    query_steamsensor = "select count(*) from {} where thingname='{}';".format(targetTable,thingName)

    check_result = []
    curr = conn.cursor()
    curr.execute(query_datapool)
    for row in curr.fetchall():
        check_result.extend(row)

    curr.execute(query_steamsensor)
    for row in curr.fetchall():
        check_result.extend(row)

    #pprint(check_result)
    return set(check_result) == set([0,0])

def initThingFirstRow(dbManager,valueStreamName,thing):
    '''
    if this thing is new, then build up first row of record into target db table.
    :param dbManager:
    :param thing:
    :return:
    '''

    '''
    select entry_id,time,property_type,property_value,property_name
    from value_stream
    where entity_id='SteamSensorValueStream'
    and source_id='SteamSensor2'
    and property_name='Pressure'
    and time < (
    select time
    from value_stream
    where entity_id='SteamSensorValueStream'
    and source_id='SteamSensor2'
    and property_name='Temperature'
    order by time desc
    limit 1
    )
    order by time desc
    limit 1;
    '''

    # step 1, get basic time from key property
    query_key_property = "select entry_id,time,property_type,property_value,property_name "
    query_key_property += "\n from value_stream "
    query_key_property += "\n where entity_id='{}' ".format(valueStreamName)
    query_key_property += "\n and source_id='{}' ".format(thing['ThingName'])
    query_key_property += "\n and property_name='{}' ".format(thing['KeyPropertyName'])
    query_key_property += "\n order by time desc limit 1;"

    #print(query_key_property)
    curr = dbManager.sourceConnection.cursor()
    curr.execute(query_key_property)
    entry_id = None
    lastTime = None
    property_type = None
    property_value = None
    property_name = None
    for row in curr.fetchall():
        entry_id, lastTime, property_type,property_value, property_name=row
        #print("{},{},{},{},{}".format(entry_id, lastTime, property_type,property_value, property_name))

    if (not entry_id) or (not lastTime):
        #this means this property doesn't have value yet in value_stream.
        # so, first row will not be created.
        #print("no data yet")
        return None, None
    #bind thing name and key property value
    fields_str = '({},{}'.format('ThingName',thing['KeyPropertyName'])
    values_str = "('{}',{}".format(thing['ThingName'],property_value)

    #combine non-key properties
    for property in thing['Properties']:
        if property['PropertyName'] != property_name:
            other_property_value=queryPropertyValueByTime(dbManager, valueStreamName, thing, lastTime, property)
            fields_str += ', {}'.format(property['PropertyName'])
            if property['PropertyType'] in ['NUMBER','INTEGER']:
                #NUMBER=1 in DB, INTEGER=22 in DB
                values_str += ',{}'.format(other_property_value)
            else:
                #BOOLEAN=2, DATETIME=3, STRING=5 in DB
                values_str += ",'{}'".format(other_property_value)
    #timestamp, lastid
    fields_str += ',{},{}'.format('lasttime','lastid')
    values_str += ",'{}',{}".format(lastTime, entry_id)

    fields_str += ")"
    values_str += ")"

    sql_insert_str = "insert into {} ".format(thing['TargetTableName'])
    sql_insert_str += "\n " + fields_str
    sql_insert_str += "\n values " + values_str
    sql_insert_str += "\n;"

    print("sql:{}".format(sql_insert_str))

    #sql to keep record in data pool
    sql_datapool = "insert into datapool (ThingName,ValueStreamName,KeyPropertyName,lastid,lasttime)"
    sql_datapool += "\n values ('{}','{}','{}',{},'{}')".format(thing['ThingName'],
                                                           valueStreamName,
                                                           thing['KeyPropertyName'],
                                                           entry_id,
                                                            lastTime)
    sql_datapool += "\n;"

    '''
    id SERIAL Primary Key,
    ThingName text NOT NULL,
    ValueStreamName text NOT NULL,
    KeyPropertyName text NOT NULL,
    lastid integer UNIQUE,
    closed boolean Default false
    '''
    return sql_insert_str, sql_datapool


def queryPropertyValueByTime(dbManager,valueStreamName,thing,lastTime,property):
    '''
    query last value of property earlier than last time.
    :param dbManager:
    :param valueStreamName:
    :param thing:
    :param lastTime:
    :param property:
    :return:
    '''
    query_property = "select property_value "
    query_property += "\n from value_stream "
    query_property += "\n where entity_id='{}' ".format(valueStreamName)
    query_property += "\n and source_id='{}' ".format(thing['ThingName'])
    query_property += "\n and property_name='{}' ".format(property['PropertyName'])
    query_property += "\n and time<='{}' ".format(lastTime)
    query_property += "\n order by time desc limit 1;"

    #print(query_property)
    curr = dbManager.sourceConnection.cursor()
    curr.execute(query_property)
    property_value = property['DefaultValue']

    for row in curr.fetchall():
        property_value = row[0]
        #print("{}:{}".format( property['PropertyName'], property_value ))

    return property_value

def verifyTargetTables(conn, owner, tables):
    '''
    verify whether required tables exist in target db or not.
    :param conn:
    :param tables:
    :return:
    '''

    query = "SELECT tablename FROM pg_catalog.pg_tables where tableowner='{}';".format(owner)
    curr = conn.cursor()
    curr.execute(query)
    target_tables = []
    for row in curr.fetchall():
        target_table = row
        target_tables.extend(target_table)
    #pprint(target_tables)
    return set(tables) == set(target_tables)

def normalizeThingRecords(dbManager,valueStreamName,thing):
    '''
    normalize Thing record when first row exists.
    :param dbManager:
    :param valueStreamName:
    :param thing:
    :return:
    '''
    #step 1 query last row in data pool
    #step 2 query latest row in value stream after last row id.
    #step 3, if there is rew record, then query target table last row as default value
    #step 4, setup empty list for all properties, including time.
    #step 5, query each property and feed in list one by one.
    #Step 6, construct sql base on all list.
    #step 1
    lastid, lasttime = queryDatapoolLastRow(dbManager,valueStreamName,thing)
    if not lastid or not lasttime:
        return None, None

    rows = queryPropertyWithTimeWindow(dbManager,valueStreamName,thing['KeyPropertyName'], lasttime, None)
    rowCount = 0
    newLatestTime = None

    total_values={}
    value_types = {}
    total_values[thing['KeyPropertyName']] = []
    total_values['lasttime'] = []
    total_values['lastid'] = []

    for row in rows:
        #print("Rows:{}".format(rowCount))
        rowCount += 1
        if rowCount < dbManager.safeBuffer+1:
            continue    #ignore latest few rows in order for safe.
        if rowCount > dbManager.safeMax:
            break   # stop to process if too many records.

        newId, newValue, newTime, newType = row
        newValue = convertPostgreSqlValueType(newValue,newType)
        if not newLatestTime:
            newLatestTime = newTime #since it is sorted by time desc, so first one is newest one

        total_values[thing['KeyPropertyName']].append(newValue)
        value_types[thing['KeyPropertyName']] = newType

        total_values['lasttime'].append(newTime)
        value_types['lasttime'] = 3
        total_values['lastid'].append(newId)
        value_types['lastid'] = 22

    if rowCount < dbManager.safeBuffer + 1:
        return None, None # return when nothing happened.

    #print("\n\n\n\nThing Name:{}, Row:{}".format(thing['ThingName'], rowCount))
    #pprint(total_values)
    default_values = queryLastNormalizedRow(dbManager,valueStreamName,thing,lastid)
    #print("\n\ndefault values")
    #pprint(default_values)

    time_bins=(total_values['lasttime'])[:] #hardcopy of time list
    time_bins.append(default_values['lasttime'])    #add old last time at the end of bins in order for performance.

    for property in thing['Properties']:
        if property['PropertyName'] == thing['KeyPropertyName']:
            continue
        total_values[property['PropertyName']], value_types = bendPropertyValueToList(
                dbManager,
                valueStreamName,
                property['PropertyName'],
                lasttime,
                newLatestTime,
                time_bins,
                default_values[property['PropertyName']],
                value_types
            )
    pprint(total_values)

    return total_values, value_types

def convertPostgreSqlValueType(property_value, property_type):
    # 1 for double, 2 for boolean, 3 for timestamp, 5 for string, 22 for integer
    if property_type == 1:
        property_value = float(property_value)
    if property_type == 22:
        property_value = int(property_value)

    return property_value

def bendPropertyValueToList(dbManager,valueStreamName,propertyName,lasttime,newLatestTime,time_bins, defaultValue,value_types):
    rows = queryPropertyWithTimeWindow(dbManager, valueStreamName, propertyName, lasttime, newLatestTime)

    # assign a list with same length as key property and time, and give default value.
    property_value_list = [defaultValue] * (len(time_bins)-1)

    #print("property_value_list before:")
    #pprint(property_value_list)
    availableIndex = 0
    property_type = None
    for row in rows:
        non_use_id, property_value, property_time,property_type = row

        # 1 for double, 2 for boolean, 3 for timestamp, 5 for string, 22 for integer
        property_value = convertPostgreSqlValueType(property_value,property_type)

        nextAvailableIndex = findIndexBinsByTime(property_time, time_bins, availableIndex) + 1
        property_value_list[availableIndex:nextAvailableIndex] = [property_value] * (
            nextAvailableIndex - availableIndex
        )
        #print("{},{},value:{},time:{}".format(availableIndex,nextAvailableIndex,property_value,property_time))
        #pprint(property_value_list)
        availableIndex = nextAvailableIndex

    value_types[propertyName] = property_type

    return property_value_list, value_types

def findIndexBinsByTime(property_time, time_bins,availableStartIndex=0):
    '''
    looking for right position of property_time in time_bins
    time_bins is descent ordered in assumption.
    :param property_time:
    :param time_bins:
    :param availableStartIndex:
    :return:
    '''
    for index in range(availableStartIndex, len(time_bins)-1):
        if time_bins[index+1]< property_time <= time_bins[index]:
            return index

    return availableStartIndex


def queryLastNormalizedRow(dbManager, valueStreamName,thing,lastid):
    '''
    query last row of normalized data as default value.
    :param dbManager:
    :param valueStreamName:
    :param thing:
    :return:
    '''
    query_str = "select {}".format(thing['KeyPropertyName'])
    query_str += ",lasttime,lastid"
    for property in thing['Properties']:
        if property['PropertyName'] == thing['KeyPropertyName']:
            continue
        query_str += ",{}".format(property['PropertyName'])

    query_str += "\nfrom {}".format(thing['TargetTableName'])
    query_str += "\nwhere lastid={};".format(lastid)
    curr = dbManager.targetConnection.cursor()
    curr.execute(query_str)
    row = curr.fetchone()

    default_values = {}
    default_values[thing['KeyPropertyName']] = row[0]
    default_values['lasttime'] = row[1]
    default_values['lastid'] = row[2]

    cellCount = 2
    for property in thing['Properties']:
        if property['PropertyName'] == thing['KeyPropertyName']:
            continue
        cellCount += 1
        default_values[property['PropertyName']] = row[cellCount]

    return default_values


def queryPropertyWithTimeWindow(dbManager, valueStreamName, propertyName, starttime,endtime=None):
    '''
    fetch property values from value stream table in source db.
    :param dbManager:
    :param valueStreamName:
    :param propertyName:
    :param starttime:
    :param endtime:
    :return:
    '''
    query_str = "select entry_id, property_value, time,property_type from value_stream"
    query_str += "\n where entity_id='{}'".format(valueStreamName)
    query_str += "\n and source_id='{}'".format(thing['ThingName'])
    query_str += "\n and property_name='{}'".format(propertyName)
    query_str += "\n and time>'{}'".format(starttime)
    if endtime:
        query_str += "\n and time<='{}'".format(endtime)
    query_str += "\norder by time desc;"

    curr = dbManager.sourceConnection.cursor()
    curr.execute(query_str)
    return curr.fetchall()

def queryDatapoolLastRow(dbManager, valueStreamName,thing):
    '''
    retrive last row of current thing in data row, None if doesn't exist.
    :param dbManager:
    :param valueStreamName:
    :param thing:
    :return:
    '''
    query_str = "select lastid, lasttime from datapool"
    query_str += "\n where valueStreamName='{}'".format(valueStreamName)
    query_str += "\n and ThingName='{}'".format(thing['ThingName'])
    query_str += "\n and KeyPropertyName='{}'".format(thing['KeyPropertyName'])
    query_str += "\n order by lasttime desc limit 1;"
    #shall we use last time instead of last id? need to check.
    lastid = None
    lasttime = None
    curr = dbManager.targetConnection.cursor()
    curr.execute(query_str)
    for row in curr.fetchall():
        lastid, lasttime = row
    return lastid, lasttime

def pushIncreamentalDataToDb(dbManager,valueStreamName, thing,total_values, value_types):
    '''
    write increamental data to target DB and datapool
    :param dbManager:
    :param valueStreamName:
    :param thing:
    :param total_values:
    :return:
    '''
    maxRows = len(total_values['lasttime'])
    if maxRows == 0:  #nothing processed
        return False

    properties_name = list(total_values.keys()) #all properties, including lasttime, lastid

    insert_sql = "insert into {} ".format(thing['TargetTableName'])
    insert_sql += "\n(ThingName," + ",".join(properties_name) + ") \nValues "
    #insert_sql += "\nvalues (" + ",".join(['%s'] * len(properties_name)) + ")"

    curr = dbManager.targetConnection.cursor()
    insert_full_sql = None
    for index in range(maxRows):
        if index % dbManager.batchSize == 0:
            # batch process
            if insert_full_sql:
                insert_full_sql += "\n;"
                print(insert_full_sql)
                curr.execute(insert_full_sql)
            insert_full_sql = insert_sql    #reset to new sql
        else:
            insert_full_sql += ","  #split differnt value set, (),() etc.

        insert_full_sql += "\n('{}'".format(thing['ThingName'])
        for property_name in properties_name:
            if value_types[property_name] == 1 or value_types[property_name] == 22:
                insert_full_sql += "," + str(total_values[property_name][index])
            else:
                insert_full_sql += ",'" + str(total_values[property_name][index]) + "'"

        insert_full_sql += ")"

        #print(insert_full_sql)
    if insert_full_sql:
        #remaining records.
        insert_full_sql += "\n;"
        print(insert_full_sql)
        curr.execute(insert_full_sql)

    '''
    id SERIAL Primary Key,
    ThingName text NOT NULL,
    ValueStreamName text NOT NULL,
    KeyPropertyName text NOT NULL,
    lastid integer UNIQUE,
    lasttime timestamp with time zone NOT NULL,
    closed boolean Default false
    '''

    datapool_insert_str = "insert into datapool (ThingName,ValueStreamName,KeyPropertyName,lastid,lasttime) "
    datapool_insert_str += "\nvalues ('{}','{}','{}',{},'{}');".format(
        thing['ThingName'],
        valueStreamName,
        thing['KeyPropertyName'],
        total_values['lastid'][0],
        total_values['lasttime'][0]
    )
    curr.execute(datapool_insert_str)

    return True


if __name__ == '__main__':
    kpiconfig = configuration.KpiConfiguration(os.path.join(SCRIPT_DIR,"../config/config.json"))
    #pprint(kpiconfig.threads)
    dbsource = kpiconfig.sourceDBConnection
    dbtarget = kpiconfig.targetDBConnection

    dbManager = db.KpiDb(dbsource,dbtarget)
    if verifyTargetTables(dbManager.targetConnection,kpiconfig.targetDBConnection['User'],
                          ['steamsensor','datapool']):
        print("oh, great, table validated.")
    else:
        print("Table Different")
        exit(1)

    for thread in kpiconfig.threads:
        for thing in thread['Things']:
            if isNewThing(dbManager.targetConnection, thread['ValueStreamName'],
                          thing['ThingName'],thing['TargetTableName']):
                insert_sql,datapool_sql = initThingFirstRow(dbManager,thread['ValueStreamName'],thing)
                if not insert_sql:
                    # no data presented in value stream yet.
                    print("no data yet for this thing.")
                else:
                    curr = dbManager.targetConnection.cursor()
                    curr.execute(insert_sql)
                    curr.execute(datapool_sql)
                    dbManager.targetConnection.commit()
                    curr.close()
            else:
                print("it's not new:")
                total_values,value_types = normalizeThingRecords(dbManager, thread['ValueStreamName'], thing)
                if total_values:
                    pprint(total_values)
                    if pushIncreamentalDataToDb(dbManager,thread['ValueStreamName'], thing,total_values,value_types):
                        #print("Finished")
                        dbManager.targetConnection.commit()
                        #curr.close()