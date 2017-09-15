from pprint import pprint
import os,sys
import pg8000 as dbapi

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import configuration

class KpiDb():
    def __init__(self):
        self.sourceConnection = None
        self.targetConnection = None

    def __init__(self, source, target, kpiconfig):
        self.__init__(source,target)
        self.setDefault(kpiconfig.safeBuffer,kpiconfig.safeMax,kpiconfig.batchSize)

    def __init__(self, source, target):
        self.sourceConnection = None
        self.targetConnection = None
        self.buildSourceConnection(
            source['DatabaseName'],
            source['Host'],
            source['Port'],
            source['User'],
            source['Password'],
            source['ssl']
        )

        self.buildTargetConnection(
            target['DatabaseName'],
            target['Host'],
            target['Port'],
            target['User'],
            target['Password'],
            target['ssl']
        )
        self.safeBuffer = 5 #5 rows will be ignored in every processing.
        self.safeMax = 6005 #6000 max rows will be processed, in order to avoid memory out.
        self.batchSize = 5  # batch insert size every time.

    def setDefault(self,safeBuffer, safeMax,batchSize):
        self.safeBuffer = safeBuffer
        self.safeMax = safeMax,
        self.batchSize = batchSize

    def getSourceConnection(self):
        if not self.sourceConnection:
            raise ValueError("Source Connection hasn't been connecged yet")

        return self.sourceConnection

    def getTargetConnection(self):
        if not self.targetConnection:
            raise ValueError("Target Connection hasn't been connected yet")

        return  self.targetConnection

    def buildSourceConnection(self,database,host,port,user,password,ssl=False):
        self.sourceConnection = self.buildConnection(
            database,host,port,user,password,ssl
        )

    def buildTargetConnection(self,database,host,port,user,password,ssl=False):
        self.targetConnection = self.buildConnection(
            database, host, port, user, password, ssl
        )

    def buildConnection(self,database,host,port,user,password,ssl=False):

        conn = None
        try:
            conn = dbapi.connect(database=database, host=host, port=port, \
                                 user=user, password=password, ssl=ssl)
        except Exception as err:
            print(err)
            raise ValueError("Error hannpened when build connection:{},{},{},{}".format(
                database,host,port,user
            ))
        return conn

def runquery(conn,query):
    """
    Just run a query given a connection
    """
    curr=conn.cursor()
    curr.execute(query)
    for row in curr.fetchall():
        pprint(row)
    return None

if __name__ == '__main__':
    kpiconfig = configuration.KpiConfiguration(os.path.join(SCRIPT_DIR,"../config/config.json"))
    #pprint(kpiconfig.threads)
    dbsource = kpiconfig.sourceDBConnection
    dbtarget = kpiconfig.targetDBConnection

    db = KpiDb(dbsource,dbtarget)

    querysource = 'select * from value_stream order by time desc limit 50;'
    querytarget = "SELECT * FROM pg_catalog.pg_tables where tableowner='kpiadmin';"
    runquery(db.sourceConnection,querysource)
    runquery(db.targetConnection,querytarget)
    #pprint(dbsource)
    #pprint(dbtarget)
