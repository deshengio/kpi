import json
from pprint import pprint

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
            for keyName in ['ThingName','TargetTableName','KeyPropertyName']:
                newThing[keyName] = thing[keyName]

            newThing['Properties']=[]
            newThing['Properties'].extend(commonProperties)
            if 'Properties' in thing:
                newThing['Properties'].extend(thing['Properties'])

            newThread['Things'].append(newThing)

        return newThread

if __name__ == '__main__':
    kpiconfig = KpiConfiguration("../config/config.json")
    pprint(kpiconfig.threads)