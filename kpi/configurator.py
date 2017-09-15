import json
from pprint import pprint
import os
import sys

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

class KpiConfiguration():
    def __init__(self):
        raise ValueError("It should be initialized from a JSON file")
    
    def __init__(self, jsonfile):
        '''
        initialize configuration object from JSON file
        :param jsonfile:
        '''
        with open(jsonfile,'r', encoding='UTF-8') as data_file:
            data = json.load(data_file)
            self.sourceDBConnection = dict(data['SourceDBConnection'])
            self.targetDBConnection = dict(data['TargetDBConnection'])
            self.safeBuffer = data.get('SafeBuffer',5)
            self.safeMax = data.get('SafeMaxRows', 6005)
            self.batchSize = data.get('BatchSize', 5)
            self.intervalCheckTime = data.get('IntervalCheckTime',60)   #default to 1 minutes.

            self.threads=[]
            for thread in data['threads']:
                self.threads.append(self.flatThingProperties(thread, data['CommonProperties']))
            #pprint(data)

    def flatThingProperties(self,thread, commonProperties):
        '''
        flat commom property to Thing property and add additional property if it has.
        :param thread:
        :return:
        '''
        newThread = {}
        newThread['ValueStreamName'] = thread['ValueStreamName']
        newThread['Things'] = []
        for thing in thread['Things']:
            newThing = {}
            newThing['ThingName'] = thing['ThingName']
            for keyName in ['ValueStreamName','TargetTableName','KeyPropertyName']:
                newThing[keyName] = thing.get(keyName,thread.get(keyName,None))

            for keyName in newThing:
                if not newThing[keyName]:
                    raise ValueError("Key:{} doesn't have right value for Thing:".format(keyName,newThing['ThingName']))

            newThing['Properties']=[]
            #newThing['Properties'].extend(commonProperties)
            thingPropertiesName = []    #filter duplicated one.
            for property in thing.get('Properties',[]):
                if not property['PropertyName'] in thingPropertiesName:
                    # New Property
                    thingPropertiesName.append(property['PropertyName'])
                    newThing['Properties'].append(property)

            #if 'Properties' in thing:
            #    newThing['Properties'].extend(thing['Properties'])
            #support additional properties definition in Thread. thread['ThreadProperties']
            for property in thread.get('ThreadProperties',[]):
                if not property['PropertyName'] in thingPropertiesName:
                    # New Property
                    thingPropertiesName.append(property['PropertyName'])
                    newThing['Properties'].append(property)

            # merge common properties
            for property in commonProperties:
                if not property['PropertyName'] in thingPropertiesName:
                    # New Property
                    thingPropertiesName.append(property['PropertyName'])
                    newThing['Properties'].append(property)

            newThread['Things'].append(newThing)

        return newThread

if __name__ == '__main__':
    kpiconfig = KpiConfiguration(os.path.join(SCRIPT_DIR,"../config/config.json"))
    pprint(kpiconfig.threads)
    print("source:{},\ntarget:{}\n".format(kpiconfig.sourceDBConnection, kpiconfig.targetDBConnection))
    print("safeBuffer:{},\tsafeMax:{},\tbatchSize:{}".format(
        kpiconfig.safeBuffer,
        kpiconfig.safeMax,
        kpiconfig.batchSize
    ))