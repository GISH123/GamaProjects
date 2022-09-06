import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.entity.entitybase import entitybase
import datetime
from package.modelmanagement.ModelException import ModelException
import json

class ModelRecordEntity (entitybase) :

    def __init__(self):
        entitybase.__init__(self)
        self.schemasName = "modelmanagement"
        self.tableName = "modelrecord"
        self.tableInfoDF = self.greenplumCtrl.getTableInfo("{}.{}".format(self.schemasName,self.tableName ))

    def makeModelRecordEntityByModelInfo(self,modelInfo):
        entity = {}
        entity["createtime"] = datetime.datetime.now()
        entity["modifytime"] = datetime.datetime.now()
        entity["deletetime"] = None
        entity["modelrecordid"] = modelInfo["modelrecordid"]
        entity["modelversion"] = modelInfo["modelversionid"]
        entity["runstep"] = ','.join(modelInfo["runStepArr"])
        entity["parameterjson"] = json.dumps(modelInfo["parameter"],ensure_ascii=False) if "parameter" in modelInfo.keys() else '{}'
        entity["resultjson"] = json.dumps(modelInfo["result"],ensure_ascii=False)  if "result" in modelInfo.keys() else '{}'
        entity["state"] = None
        return entity







