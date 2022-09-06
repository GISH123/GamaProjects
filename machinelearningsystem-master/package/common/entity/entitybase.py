import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.postgreCtrl import PostgresCtrl
from dotenv import load_dotenv
import pandas

load_dotenv(dotenv_path="env/greenplus.env")

class entitybase(object):

    def __init__(self):
        self.greenplumCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="mlops"
            , schema="public"
        )
        self.entity = {}
        self.schemasName = ""
        self.tableName = ""
        self.tableInfoDF = None

    def getEntity(self):
        return self.entity

    def setEntity(self, entity):
        self.entity = {}
        for tableInfoIndex, tableInfoRow in self.tableInfoDF.iterrows():
            if tableInfoRow["columnname"] in entity.keys() :
                self.entity[tableInfoRow["columnname"]] = entity[tableInfoRow["columnname"]]

    def getColumnValue(self , columnName):
        return self.entity[columnName]

    def setColumnValue(self, columnName , columnValue):
        self.entity[columnName] = columnValue

    def insertEntity(self):
        self.entity["{}id".format(self.tableName)] = self.getNextPrimaryKeyId()
        df = pandas.DataFrame([self.entity])
        self.greenplumCtrl.insertData(tableFullName="{}.{}".format(self.schemasName,self.tableName), insertTableInfoDF=self.tableInfoDF , insertDataDF=df)

    def updateEntity(self):
        self.greenplumCtrl.updateData(tableFullName="{}.{}".format(self.schemasName, self.tableName), updateTableInfoDF=self.tableInfoDF, updateData=self.entity)

    def insertEntityList(self , entityList):
        newEntityList = []
        for entity in entityList :
            entity["{}id".format(self.tableName)] = self.getNextPrimaryKeyId()
            newEntityList.append(entity)
        df = pandas.DataFrame(newEntityList)
        self.greenplumCtrl.insertData(tableFullName="{}.{}".format(self.schemasName, self.tableName), insertTableInfoDF=self.tableInfoDF, insertDataDF=df)

    def getNextPrimaryKeyId(self):
        sql = " SELECT nextval('{}.{}_{}id_seq') ".format(self.schemasName, self.tableName, self.tableName)
        df = self.greenplumCtrl.searchSQL(sql)
        return df['nextval'][0]

    def getEntityByPrimaryKeyId(self, primaryKeyId):
        sql = """
           SELECT * 
           FROM [:SchemasName].[:TableName] AA
           WHERE 1 = 1 
               AND AA.[:TableName]id = [:PrimaryKeyId]
           limit 1 
        """.replace('[:SchemasName]', self.schemasName) \
            .replace('[:TableName]', self.tableName) \
            .replace('[:PrimaryKeyId]', str(primaryKeyId))
        df = self.greenplumCtrl.searchSQL(sql)
        self.setEntity(df.iloc[0].to_dict())
        return self.entity

