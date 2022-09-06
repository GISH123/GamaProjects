import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.entity.entitybase import entitybase
import datetime
import pandas
import numpy
import math

class ModelDataCheckInfoEntity (entitybase) :

    def __init__(self):
        entitybase.__init__(self)
        self.schemasName = "modeldatacheck"
        self.tableName = "modeldatacheckinfo"
        self.tableInfoDF = self.greenplumCtrl.getTableInfo("{}.{}".format(self.schemasName,self.tableName ))

    def deleteCheckSQL(self, checkInfo):
        productName = checkInfo["productName"]
        project = checkInfo["project"]
        step = checkInfo["step"]
        version = checkInfo["version"]
        makeTime = checkInfo["makeTime"]

        deleteSQL = """
        UPDATE modeldatacheck.modeldatacheckinfo 
        SET deletetime = now()
        WHERE 1 = 1 
            AND deletetime is null 
            AND productname = '[:ProductName]'
            AND project = '[:Project]'
            AND step = '[:Step]'
            AND version = '[:Version]'
            AND datadate = '[:DataDate]'
        """.replace("[:ProductName]", productName) \
            .replace("[:Project]", project) \
            .replace("[:Step]", step) \
            .replace("[:Version]", version) \
            .replace("[:DataDate]", datetime.datetime.strptime(makeTime, "%Y-%m-%d").strftime("%Y/%m/%d"))
        self.greenplumCtrl.executeSQL(deleteSQL)


    def makeModelDataCheckInfoEntityByCheckInfo(self,modelDataCheckInfo):
        entity = {}
        entity["createtime"] = datetime.datetime.now()
        entity["modifytime"] = datetime.datetime.now()
        entity["deletetime"] = None
        entity["modeldatacheckinfoid"] = None
        entity["productname"] = modelDataCheckInfo["productname"]
        entity["project"] = modelDataCheckInfo["project"]
        entity["step"] = modelDataCheckInfo["step"]
        entity["version"] = modelDataCheckInfo["version"]
        entity["datadate"] = datetime.datetime.strptime(modelDataCheckInfo["datadate"], "%Y%m%d").strftime("%Y/%m/%d")
        entity["checkcolumn"] = modelDataCheckInfo["checkcolumn"]
        entity["checkfunc"] = modelDataCheckInfo["checkfunc"]
        entity["checknumbervalue"] = None if numpy.isnan(modelDataCheckInfo["checknumbervalue"]) else modelDataCheckInfo["checknumbervalue"]
        entity["checktextvalue"] = None if modelDataCheckInfo["checktextvalue"] == None else modelDataCheckInfo["checktextvalue"]

        return entity



