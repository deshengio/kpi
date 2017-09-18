import pg8000 as dbapi

class Property():
    def __init__(self, PropertyName,PropertyType,DefaultValue):
        self.PropertyName = PropertyName
        self.PropertyType = PropertyType
        self.DefaultValue = DefaultValue

    def __eq__(self,other):
        return self.PropertyName == other.PropertyName

    def description(self):
        return "{PropertyName:{},PropertyType:();DefaultValue:{}}".format(
            self.PropertyName,
            self.PropertyType,
            self.DefaultValue
        )
    def __str__(self):
        return self.description()

    def __repr__(self):
        return self.description()

class Thing():
    #Thing object. which will hold properties
    def __init__(self, ThingName, TargetTableName, KeyPropertyName, ValueStreamName):
        self.ThingName = ThingName
        self.TargetTableName = TargetTableName
        self.KeyPropertyName = KeyPropertyName
        self.ValueStreamName = ValueStreamName
        self.Properties = []
        self.PropertiesName = []

    def isKeyProperty(self,property):
        return self.KeyPropertyName == property.PropertyName

    def addProperty(self, property):
        if not property.PropertyName in self.PropertiesName:
            self.Properties.append(property)
            self.PropertiesName.append(property.PropertyName)

    def description(self):
        str = "ThingName:{}".format(self.ThingName)
        str += "\n\tTargetTableName:{};KeyPropertyName:{};ValueStreamName:{}".format(
            self.TargetTableName,
            self.KeyPropertyName,
            self.ValueStreamName
        )
        for property in self.PropertiesName:
            str += "\n\t\t" + property
        return str

    def __str__(self):
        return self.description()

    def __repr__(self):
        return self.description()

class DBConnection():
    def __init__(self,DatabaseName,Host,Port,User,Password,ssl ):
        self.DatabaseName = DatabaseName
        self.Host = Host
        self.Port = Port
        self.User = User
        self.Password = Password
        self.ssl = ssl
        self.conn = None
    def description(self):
        return "user:{};database:{};Host:{};Port:{};ssl:{}".format(
            self.User,
            self.DatabaseName,
            self.Host,
            self.Port,
            self.ssl
        )

    def __str__(self):
        return self.description()

    def __repr__(self):
        return self.description()

    def getDBConnection(self):
        if not self.conn:
            try:
                self.conn = dbapi.connect(database=self.DatabaseName,
                                     host=self.Host,
                                     port=self.Port,
                                     user=self.User,
                                     password=self.Password,
                                     ssl=self.ssl)
            except Exception as err:
                print(err)
                raise ValueError("Error hannpened when build connection:{},{},{},{}".format(
                    self.DatabaseName, self.Host, self.Port, self.User
                ))

        return self.conn

class Thread():
    def __init__(self, ThreadName):
        self.ThreadName = ThreadName
        self.Things = []
        self.ThingsName = []
        self.AutoCreateTable = False
        self.VerifyTableStructure = False
        self.SafeBuffer = 5
        self.SafeMaxRows = 1005
        self.BatchSize = 5

    def setDBConnection(self,sourceDBConnection, targetDBConnection):
        self.SourceDB = DBConnection(
            DatabaseName=sourceDBConnection['DatabaseName'],
            Host=sourceDBConnection['Host'],
            Port=sourceDBConnection['Port'],
            User=sourceDBConnection['User'],
            Password=sourceDBConnection['Password'],
            ssl=sourceDBConnection['ssl']
        )
        self.TargetDB = DBConnection(
            DatabaseName=targetDBConnection['DatabaseName'],
            Host=targetDBConnection['Host'],
            Port=targetDBConnection['Port'],
            User=targetDBConnection['User'],
            Password=targetDBConnection['Password'],
            ssl=targetDBConnection['ssl']
        )
        self.AutoCreateTable = targetDBConnection['AutoCreateTable']
        self.VerifyTableStructure = targetDBConnection['VerifyTableStructure']

    def setConstraints(self,buffer,maxRows,batch):
        self.SafeBuffer = buffer
        self.SafeMaxRows = maxRows
        self.BatchSize = batch

    def setChecks(self, autocreate, verifytable):
        self.AutoCreateTable = autocreate
        self.VerifyTableStructure = verifytable

    def addThing(self,thing):
        if isinstance(thing, Thing):
            if not thing.ThingName in self.ThingsName:
                self.ThingsName.append(thing.ThingName)
                self.Things.append(thing)

    def description(self):
        str = "Thread:{} has Things:{}".format(self.ThreadName,len(self.Things))
        str += "\n\tAutoCreate:{};VerifyTable:{}".format(self.AutoCreateTable,self.VerifyTableStructure)
        str += "\n\tBuffer:{};MaxRows:{};BatchSize:{}".format(self.SafeBuffer,self.SafeMaxRows,self.BatchSize)
        str += "\n\tSource DB:{}".format(self.SourceDB)
        str += "\n\tTarget DB:{}".format(self.TargetDB)
        for thing in self.Things:
            str += "\n\tThing:{}".format(thing)
        return str

    def __str__(self):
        return self.description()

    def __repr__(self):
        return self.description()