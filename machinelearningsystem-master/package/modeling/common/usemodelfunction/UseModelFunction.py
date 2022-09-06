import os
from package.modeling.common.common.CommonFunction import CommonFunction
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv

class UseModelFunction(CommonFunction):

    def __init__(self):
        pass

    def getTagXYData(self,makeInfo):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )

        columnNameArr = self.getUseModelColumnNameArr()

        columnsSQL = ""
        for columnName in columnNameArr :
            columnSQL = "\n                , SUM(CASE WHEN AA.{} > 0 then 1 else 0 end ) as {}".format(columnName,columnName)
            columnsSQL = columnsSQL + columnSQL

        wheresSQL = ""
        for whereInfo in makeInfo['makedatainfo']['xinfoarr']:
            whereSQL = "\n                    OR (AA.product = '{}' AND AA.project='{}' AND AA.version='{}' AND AA.step='{}' AND AA.dt = '{}')" \
                        .format(whereInfo["product"],whereInfo["project"],whereInfo["version"],whereInfo["step"],whereInfo["dt"])
            wheresSQL = wheresSQL + whereSQL
        for whereInfo in [makeInfo['makedatainfo']['yinfo']]:
            whereSQL = "\n                    OR (AA.product = '{}' AND AA.project='{}' AND AA.version='{}' AND AA.step='{}' AND AA.dt = '{}')" \
                        .format(whereInfo["product"],whereInfo["project"],whereInfo["version"],whereInfo["step"],whereInfo["dt"])
            wheresSQL = wheresSQL + whereSQL

        sql = """
            SELECT
                AA.product
                , AA.project
                , AA.version
                , AA.step
                , AA.dt
                , AA.commondata_013 [:ColumnsSQL] 
            FROM gtwpd.model_usedata AA
            where 1 = 1
                AND ( 1 != 1 [:WheresSQL] 
                )
            GROUP BY
                AA.product
                , AA.project
                , AA.version
                , AA.step
                , AA.dt
                , AA.commondata_013
        """.replace("[:ColumnsSQL]", columnsSQL).replace("[:WheresSQL]", wheresSQL)
        print(sql)

        dataDF = hiveCtrl.searchSQL(sql)

        print(dataDF)
        return None


