import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.entity.entitybase import entitybase
import datetime
from package.modelmanagement.ModelException import ModelException
import json

class ModelVersionEntity (entitybase) :

    def __init__(self):
        entitybase.__init__(self)
        self.schemasName = "modelmanagement"
        self.tableName = "modelversion"
        self.tableInfoDF = self.greenplumCtrl.getTableInfo("{}.{}".format(self.schemasName,self.tableName ))

    def getModelVersionByProductNameProjectModelVersion(self, productName, project, modelVersion):
        sql = """
               SELECT * 
               FROM modelmanagement.modelversion AA
               WHERE 1 = 1 
                   AND AA.productname = '[:ProductName]'
                   AND AA.project = '[:Project]'
                   AND AA.modelversion = '[:ModelVersion]'
                   AND deletetime is null 
               order by 
	               modifytime DESC 
               limit 1 
           """.replace('[:ProductName]', productName).replace('[:Project]', project).replace('[:ModelVersion]', modelVersion)
        df = self.greenplumCtrl.searchSQL(sql)
        if len(df) == 0 :
            raise ModelException("Not find {} {} {} Model".format(productName,project,modelVersion))
        self.setEntity(df.iloc[0].to_dict())
        return self.entity

    def getLastModelVersionNo(self,productName, project):
        sql = """
        SELECT 
            CASE WHEN MAX(modelversion) IS NULL THEN 'V0_0_0' ELSE MAX(modelversion) END AS maxmodelversion
        FROM ( 
            SELECT 
                ROW_NUMBER() over( ORDER bY 
                    cast(split_part(regexp_replace(modelversion,'V',''),'_',1) AS integer ) DESC
                    , cast(split_part(regexp_replace(modelversion,'V',''),'_',2) AS integer ) DESC
                    , cast(split_part(regexp_replace(modelversion,'V',''),'_',3) AS integer ) DESC
                    , createtime DESC) AS rownumber
                , modelversion
            FROM modelmanagement.modelversion AA
            WHERE 1 = 1 
                AND AA.productname = '[:ProductName]'
                AND AA.project = '[:Project]'
        ) AAA
        WHERE 1 = 1 
            AND AAA.rownumber = 1
        """.replace('[:ProductName]', productName).replace('[:Project]', project)
        df = self.greenplumCtrl.searchSQL(sql)
        return df['maxmodelversion'][0]

    def deleteOldModelVersionByModelInfo(self,modelInfo) :
        sql = """
        update modelmanagement.modelversion 
        set deletetime  = now()
        where 1 = 1 
            and productname	 = '[:ProductName]'
            and project = '[:Project]'
            and deletetime is null 
            and modelversion = '[:ModelVersion]'
        """.replace('[:ProductName]', modelInfo["productName"]).replace('[:Project]', modelInfo["project"]).replace('[:ModelVersion]', modelInfo["modelVersion"])
        self.greenplumCtrl.executeSQL(sql)

    def makeModelVersionEntityByModelInfo(self,modelInfo):
        entity = {}
        entity["createtime"] = datetime.datetime.now()
        entity["modifytime"] = datetime.datetime.now()
        entity["deletetime"] = None
        entity["modelversionid"] = None
        entity["builduser"] = modelInfo["buildUser"]
        entity["builddate"] = modelInfo["makeTime"]
        entity["productname"] = modelInfo["productName"]
        entity["project"] = modelInfo["project"]
        entity["modelversion"] = modelInfo["modelVersion"]
        entity["modelversionfullcode"] = modelInfo["modelVersionFullCode"]
        entity["usemodelversion"] = modelInfo["usemodelVersion"]
        entity["preprocessversion"] = modelInfo["preprocessVersion"]
        entity["rawdataversion"] = modelInfo["rawdataVersion"]
        entity["runstep"] = ','.join(modelInfo["runStepArr"])
        entity["parameterjson"] = json.dumps(modelInfo["parameter"],ensure_ascii=False) if "parameter" in modelInfo.keys() else '{}'
        entity["resultjson"] = json.dumps(modelInfo["result"],ensure_ascii=False) if "result" in modelInfo.keys() else '{}'
        return entity







