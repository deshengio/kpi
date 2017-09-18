import json
from pprint import pprint
import os
import sys

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

try:
    from . import thingmodules
except ImportError:
    import thingmodules

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
            self.intervalCheckTime = data.get('IntervalCheckTime',60)   #default to 1 minutes.

            self.threads=[]
            for thread in data['threads']:
                newThread = self.flatThingProperties(thread, data['CommonProperties'])
                newThread.setDBConnection(dict(data['SourceDBConnection']), dict(data['TargetDBConnection']))
                newThread.setConstraints(data.get('SafeBuffer',5),
                                         data.get('SafeMaxRows', 6005),
                                         data.get('BatchSize', 5))
                self.threads.append(newThread)

    def flatThingProperties(self,thread, commonProperties):
        '''
        flat commom property to Thing property and add additional property if it has.
        :param thread:
        :return:
        '''
        newThread = thingmodules.Thread(thread.get("ThreadName",""))

        for thing in thread['Things']:
            newThing = thingmodules.Thing(ThingName=thing['ThingName'],
                                          TargetTableName=thing.get('TargetTableName',
                                                                    thread.get('TargetTableName',None)),
                                          KeyPropertyName=thing.get('KeyPropertyName',
                                                                    thread.get('KeyPropertyName',None)),
                                          ValueStreamName=thing.get('ValueStreamName',
                                                                    thread.get('ValueStreamName',None)))

            for property in thing.get('Properties',[]):
                #pprint(property)
                newProperty = thingmodules.Property(
                    PropertyName = property['PropertyName'],
                    PropertyType = property['PropertyType'],
                    DefaultValue = property['DefaultValue']
                )
                newThing.addProperty(newProperty)

            #if 'Properties' in thing:
            #    newThing['Properties'].extend(thing['Properties'])
            #support additional properties definition in Thread. thread['ThreadProperties']
            for property in thread.get('ThreadProperties',[]):
                #pprint(property)
                newProperty = thingmodules.Property(
                    PropertyName=property['PropertyName'],
                    PropertyType=property['PropertyType'],
                    DefaultValue=property['DefaultValue']
                )
                newThing.addProperty(newProperty)

            # merge common properties
            for property in commonProperties:
                newProperty = thingmodules.Property(
                    PropertyName=property['PropertyName'],
                    PropertyType=property['PropertyType'],
                    DefaultValue=property['DefaultValue']
                )
                newThing.addProperty(newProperty)

            newThread.addThing(newThing)

        return newThread

if __name__ == '__main__':
    kpiconfig = KpiConfiguration(os.path.join(SCRIPT_DIR,"../config/config.json"))
    pprint(kpiconfig.threads)
