import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from dotenv import load_dotenv
import datetime as dt
import pandas as pd
import numpy as np
import sys, time, warnings, scipy
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
import package.common.analysis.norm as norm
import package.common.analysis.entropy as etp
import package.common.analysis.pca_process as pca
#####################################################################################################################
tryCount = 3
runDates = [2]
###########################
load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)


load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(
      url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)

load_dotenv(dotenv_path="env/PostgreSQL.env")
pgsql = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=int(os.getenv("POSTGRES_PORT"))
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database='maple'
    , schema='model_rua'
)
pgsql2 = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=int(os.getenv("POSTGRES_PORT"))
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database='maple'
    , schema='result_rua'
)
def main(parametersData = {}):
    nowTime = dt.datetime.now()
    print('start at : ' + nowTime.strftime("%T"))
    print('running: P70_UseModel.py')
    nowZeroTime = dt.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)
    world = ["45", "06", "04", "03", "02", "01" , "00"]
    for key in parametersData.keys():
        if key == "world":
            world = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key in ('dt', 'makedate'):
            startDateStr = parametersData[key][0]
            endDateStr = parametersData[key][0]

    startDateTime = dt.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = dt.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:
        print(makeTime)
        if makeTime.day in runDates:
            monthFirstDay = makeTime - dt.timedelta(days=30)
            monthFirstDay = dt.datetime(monthFirstDay.year, monthFirstDay.month, 1, 0, 0, 0, 0)
            joinTables(monthFirstDay, world)
            preProcess(monthFirstDay, world)
            predictMain(monthFirstDay, world)
            uploadTableRaw(monthFirstDay, world)
            uploadTableGroup(monthFirstDay, world)
        makeTime = makeTime + dt.timedelta(days=1)

# 建立日期List，獲取每個月的日期

def joinTables(makeTime,gameWorldList):
    path = 'file/maple/RUAmodel_SQL/'
    tableName = 'bigt2_3_0'  # 輸出表格的名字
    sqlfile = 'joinALL.sql'
    nextTime = makeTime + dt.timedelta(days=31)
    nextTime = dt.datetime(nextTime.year, nextTime.month, 1, 0, 0, 0, 0)
    d_str = makeTime.strftime("%Y%m%d")
    for gw in gameWorldList:

        # 建立需要執行的SQL List
        # 執行每日之sql

        sqlReplaceArr = [['[:gw]', gw],
                         ['[:gwInt]',str(int(gw))],
                         ['[:Date0]', d_str],
                         ['[:DateN]', (nextTime - dt.timedelta(days=1)).strftime("%Y%m%d")],
                         ['[:DateN+1]', nextTime.strftime("%Y%m%d")],
                         ['[:Date-N+1]', nextTime.strftime("%Y-%m-%d")],
                         ['[:tb]', tableName]
                         ]

        sqlStrArr = SqlTool().loadfile(path  + sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
        fileName = f'{tableName}_gw{gw}_{d_str}'

        print(sqlStrArr[0])
        df = hiveCtrl.searchSQL_TCByCount(sqlStrArr[0],3)
        pgsql.writeDFToTable(df, fileName, 'replace')
        print(f"gw{gw} has downloeaded !")
def preProcess(strDate , gameWorldList):
    #warnings.filterwarnings('ignore', category=RuntimeWarning)
    #warnings.filterwarnings('ignore', category=FutureWarning)
    warnings.filterwarnings('ignore')
    path = 'file/maple/RUAmodel_SQL/'
    InputTableName = 'bigt2_3_0'  # Input表格名
    OutputTableName = 'new2_3_0'  # Output表格名
    sqlfile = r'downloadTable.sql'
    schema = 'model_rua'
    # 預處理的欄位們
    tfCOL = ['i_atk'] # 做閾值處理的欄位們
    linearCOL = ['onlinedays', 'level_f','gw_count'] # 線性處理的欄位們
    logCOL = ['c_albumsize', 'c_friendnum', 'buy_volumn', 'sell_volumn','q_all', 'g_commit_f', 'g_commit_gain', 'money_avg',
              'money_sd', 'age', 'acc_num','i_acc', 'i_spl', 'i_pzl', 'i_def', 'ic_inv',
              'l_sun','l_mon', 'l_tue', 'l_wed','l_thu','l_fri','l_sat',
              'buy_item','sell_item','buy_gash','gift_gash',
              'auc_eqp_buy', 'auc_eqp_sell', 'auc_fam_buy', 'auc_fam_sell', 'auc_oth_buy', 'auc_oth_sell',
              'auc_cash_buy', 'auc_cash_sell'
              ] # 對數處理的欄位們

    logMovCOL = ['pop_f', 'exp_gain'] # 雙對數處理的欄位們

    nfCOL = ['s_charismaexp', 's_insightexp', 's_willexp', 's_craftexp', 's_senseexp', 's_charmexp'] # 非戰鬥數值相關的欄位們
    fCOL = ['m_mhp', 'm_str', 'm_int', 'm_luk', 'm_statdamagemin', 'm_statdamagemax', 'm_bossdamager', 'm_ignoredef'] # 戰鬥數值相關的欄位們

    saveCOL = ['gw', 'week', 'characterid', 'accountid' ,'c_name', 'g_grade', 'q_important', 'x_g_commit_gain', 'time_ratio',
               'x_g_grade1', 'x_g_grade2', 'x_g_grade3', 'x_g_grade0','b_job'] # 最後其他要保留的欄位們

    # 輸出時的欄位們
    finalCOL = saveCOL + logCOL + tfCOL + linearCOL + logMovCOL + nfCOL + fCOL

    for gw in gameWorldList:
            d = strDate
            # 讀資料庫
            sqlReplaceArr = [['[:GW]', f'gw{gw}'],
                             ['[:Date0]', d.strftime("%Y%m%d")],
                             ['[:tb]', InputTableName],
                             ['[:sc]', schema]]
            sqlStrArr = SqlTool().loadfile(path +sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
            bigTable = pgsql.searchSQL(sqlStrArr[0])

            bigTable.fillna(0, inplace=True)

            # 創建與資料標頭有關的欄位

            bigTable['gw'] = f'gw{gw}'
            bigTable['week'] = d.strftime("%Y%m%d")

            # 創建與增加量有關的欄位
            bigTable['exp_gain'] = bigTable['exp_gain'] / bigTable['exp_f']
            bigTable.loc[bigTable['g_commit_gain'] < 0, 'g_commit_gain'] = 0

            # 任務相關欄位的預處理
            bigTable['q_all'] = bigTable['q_daily'] + bigTable['q_milestone'] + bigTable['q_mday'] + bigTable['q_other'] + \
                                bigTable['q_happyday'] + bigTable['q_battlefield'] + bigTable['q_wulin']

            bigTable['q_important'] = 1 - bigTable['q_other'] / bigTable['q_all']
            bigTable['q_important'].fillna(0, inplace=True)

            # 解釋用的變數
            bigTable['x_g_commit_gain'] = bigTable['g_commit_gain'].copy()
            bigTable['x_g_grade1'] = 1 * (bigTable['g_grade'] == 1)
            bigTable['x_g_grade2'] = 1 * (bigTable['g_grade'] == 2)
            bigTable['x_g_grade3'] = 1 * np.logical_and(bigTable['g_grade'] >= 3, bigTable['g_grade'] <= 5)
            bigTable['x_g_grade0'] = 1 * np.logical_or(bigTable['g_grade'] == 0, bigTable['g_grade'] == 6)

            # 公會等級轉換: 1->1, 2->0.5, 其他->0
            bigTable.loc[bigTable['g_grade'] == 0, 'g_grade'] = 6
            bigTable['g_grade'] = 2 // bigTable['g_grade'] / 2

            # 批次歸一化
            for col in (fCOL + nfCOL):
                bigTable[col] = norm.groupNorm(bigTable[col].copy(), bigTable['level_f'] // 10, True)
            for col in tfCOL:
                bigTable[col] = norm.tfNorm(bigTable[col])
            for col in linearCOL:
                bigTable[col] = norm.Norm(bigTable[col])
            for col in logCOL:
                bigTable[col] = norm.logNorm(bigTable[col])
            for col in logMovCOL:
                bigTable[col] = norm.DublogNorm(bigTable[col])

            bigTable.fillna(value=0, inplace=True)

            pgsql.writeDFToTable(bigTable[finalCOL], "{0}_gw{1}_{2}".format(OutputTableName,gw,d.strftime("%Y%m%d")),
                                 if_exists='replace')
def predictMain(strDate , gameWorldList):
    tableName = 'new2_3_0'
    modelName = 'rua2_3_2'
    path = 'file/maple/RUAmodel_SQL/'
    sqlfile = r'downloadTable.sql'
    d = strDate
    d_str = d.strftime("%Y%m%d")

    # 名字字典相關檔案

    sqltxt = f"SELECT * FROM result_rua.param_result WHERE modelname = '{modelName}' AND modeltype = 'dict' " \
             f"ORDER BY createtime DESC LIMIT 1;"
    param_table = pgsql.searchSQL(sqltxt)

    wordDict =  ''.join(param_table['metadata'])

    # 處理PCA
    pcaNames = ['F', 'NF', 'item','wd','auc_buy','auc_sell']
    pcaColDict= {}
    pcaContentDict= {}
    for i in pcaNames:
        cols, content =pca.pca_readSQL(pgsql, modelName, i)
        pcaColDict.update({i : cols})
        pcaContentDict.update({i : content})

    # 抓模型回來
    sqltxt = f"SELECT * FROM result_rua.{modelName}_center;"
    kmeans_table = pgsql.searchSQL(sqltxt)

    data = None
    infoData = None

    infoNames = ['gw', 'week', 'characterid', 'accountid', 'c_name','b_job']
    supportNames = ['x_g_commit_gain', 'x_g_grade1', 'x_g_grade2', 'x_g_grade3', 'x_g_grade0']
    pcaNames = ['F', 'NF', 'item','wd','auc_buy','auc_sell']

    for gw in gameWorldList:
        sqlReplaceArr = [['[:GW]', f'gw{gw}'], ['[:Date0]', d_str], ['[:tb]', tableName],
                         ['[:sc]', 'model_rua']]
        sqlStrArr = SqlTool().loadfile(path+sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()

        tmp = pgsql.searchSQL(sqlStrArr[0])
        tmp = pca.pca_use(pcaContentDict, pcaColDict, tmp)

        # 處理
        tmp["nameEntropy"] = norm.Norm(np.array(etp.wordListEntropy(tmp['c_name'], ''.join(wordDict), byDict=False)))

        # 備份資料
        infoData = tmp[infoNames+ supportNames ].copy() if infoData is None \
            else infoData.append(tmp[infoNames + supportNames].copy(), ignore_index=True,sort=False)

        tmp.drop(infoNames + supportNames, axis=1, inplace=True)
        data = tmp if data is None else data.append(tmp, ignore_index=True,sort=False)


    # 找最接近類別
    dists = pd.DataFrame(scipy.spatial.distance.cdist(data, kmeans_table))

    infoData['cluster'] = dists.idxmin(axis=1) + 1
    infoNames.append('cluster')

    pcaNames = [ f'{p}1' for p in pcaNames]
    print(infoData.columns)
    output =pd.concat([infoData[infoNames],data[pcaNames+['nameEntropy']]],axis = 1)
    pgsql2.writeDFToTable(output, modelName+ '_group_' + d_str, 'replace')
def uploadTableGroup(strDate , gwList):
    hive_tableName = 'group'
    pd_tableName = 'rua2_3_2_group'
    version = 'v2_3_2'
    d = strDate
    d_str = d.strftime('%Y%m%d')
    for gw in gwList:
        tbName = f"{hive_tableName}_gw{gw}_{d_str}"
        hdfsPath = f"/user/GTW_PD/DB/Maple/resultrua/{hive_tableName}/version={version}/dt={d_str}/world={gw}/"
        hdfsCtrl.deleteDirs(hdfsPath)
        df = pgsql.searchSQL(f"SELECT * FROM result_rua.{pd_tableName}_{d_str} WHERE gw = 'gw{gw}'")
        df.to_csv(tbName,index=False, header= False)
        hdfsCtrl.upload(hdfsPath + tbName + ".csv",tbName)
        os.remove(tbName)
    hiveCtrl.executeSQL(f'MSCK REPAIR TABLE gtwpd.maple_resultrua_{hive_tableName}')

def uploadTableRaw(strDate , gwList):
    hive_tableName = 'raw_data'
    pd_tableName = 'bigt2_3_0'

    d = strDate
    d_str = d.strftime('%Y%m%d')
    for gw in gwList:
        tbName = f"{pd_tableName}_gw{gw}_{d_str}"
        hdfsPath = f"/user/GTW_PD/DB/Maple/modelrua/{hive_tableName}/dt={d_str}/world={gw}/"
        hdfsCtrl.deleteDirs(hdfsPath)

        df = pgsql.searchSQL(f"SELECT * FROM model_rua.{pd_tableName}_gw{gw}_{d_str}")
        df.to_csv(tbName,index=False, header= False)
        hdfsCtrl.upload(hdfsPath + tbName +  ".csv",tbName)
        os.remove(tbName)

        hiveCtrl.executeSQL(f'MSCK REPAIR TABLE gtwpd.maple_modelrua_{hive_tableName}')
if __name__ == "__main__":
    print('start P70_runJoinSQL')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
