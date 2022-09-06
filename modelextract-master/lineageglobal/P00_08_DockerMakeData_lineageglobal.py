import os, sys, time; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.asyncio.asyncioSSHTool import AsyncioSSHTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/Telegram.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")

asyncioSSHTool = AsyncioSSHTool()

sshCtrl_ssh = SSHCtrl(
    host="10.10.99.138"
    , port=22
    , usr="root"
    , pkey="env/ALL_PKEY_HOST"
)
# Docker資訊
dockerName = "pythonbd_ice"
dockerFolder = "PythonBD_ICE"

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    dateRangeArr = []
    gameNameArr = []
    tableNumberArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "dateRangeArr":
            dateRangeArr = parametersData[key]
        if key == "gamename":
            gameNameArr = parametersData[key]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]

    gameNameArr = ["lineageglobal"] if gameNameArr == [] else gameNameArr

    # 準備環境
    for gameName in gameNameArr:
        sshStrs = """
            rm -rf /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]
            git clone http://10.10.99.191:8888/gtw_pd/ModelExtract.git /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]
            cp -r /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName] /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]_copy 
            cp -r /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]_copy /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]/[:GameName]
            cp -r /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/package /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]/
            cp -r /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/sql /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]/
            cp -r /root/config/modelextract /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]/env
            cp -r /root/config/modelextract /dfs01/Docker/[:DockerFolder]/Volumes/Data/DockerMakeModelExtract_[:GameName]/[:GameName]/[:GameName]/env
        """.replace("[:GameName]", gameName).replace("[:DockerFolder]", dockerFolder)
        for sshStr in sshStrs.split("\n"):
            # print(sshStr)
            sshCtrl_ssh.ssh_exec_cmd(sshStr)

    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            mainALLData(makeDateStr, gameNameArr, tableNumberArr)
    elif dateRangeArr != []:
        for makeDateRange in dateRangeArr:
            startDateTime = datetime.datetime.strptime(makeDateRange[0], "%Y-%m-%d")
            endDateTime = datetime.datetime.strptime(makeDateRange[1], "%Y-%m-%d")
            makeDateStrArr = []
            makeDatetime = startDateTime
            while makeDatetime <= endDateTime:
                makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
                makeDatetime = makeDatetime + datetime.timedelta(days=1)
            for makeDateStr in makeDateStrArr:
                mainALLData(makeDateStr, gameNameArr, tableNumberArr)
    else:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
        for makeDateStr in makeDateStrArr:
            mainALLData(makeDateStr, gameNameArr, tableNumberArr)


def mainALLData(makeDateStr, gameNameArr, tableNumberArr):

    makeInfo = {
        "makeName": "ModelExtract"
        , "makeDateStr": makeDateStr
        , "gameNameArr": gameNameArr
        , "tableNumberArr": tableNumberArr
    }

    gameTaskSSHStrArrMap = makeSSHStrArr(makeInfo)
    gameTaskSSHStrArr = []
    for gameName in gameNameArr:
        for gameTaskSSHStr in gameTaskSSHStrArrMap[gameName]:
            gameTaskSSHStrArr.append(gameTaskSSHStr)
    if gameTaskSSHStrArr != []:
        '''for gameTaskSSHStr in gameTaskSSHStrArr:
            print(gameTaskSSHStr)'''
        asyncioSSHTool.runSSH(sshCtrl_ssh, gameTaskSSHStrArr, printError=True)


def makeSSHStrArr(makeInfo):
    makeDateStr = makeInfo["makeDateStr"]
    gameNameArr = makeInfo["gameNameArr"]
    tableNumberArr = makeInfo["tableNumberArr"]
    gameTaskSSHStrArrMap = {}
    sshStrsInit = """docker exec -it [:DockerName] python3 /Data/DockerMakeModelExtract_[:GameName]/[:GameName]/P00_01_Run_lineageglobal.py [:Parameter] """.replace("[:DockerName]", dockerName)
    parameterStringInit = "--makedate [:MakeDataStr] --gamename [:GameNameStr] "
    if tableNumberArr != []:
        parameterStringInit = parameterStringInit + "--tableNumberArr [:TableNumberArr] "

    for gameName in gameNameArr:
        gameTaskSSHStrArrMap[gameName] = []
        parameterString = parameterStringInit.replace("[:MakeDataStr]", makeDateStr).replace("[:GameNameStr]", gameName)
        if tableNumberArr != []:
            tableNumberstrs = ""
            for tableNumber in tableNumberArr:
                tableNumberstrs = "{}".format(tableNumber) if tableNumberstrs == "" else tableNumberstrs + " {}".format(tableNumber)
            parameterString = parameterString.replace("[:TableNumberArr]", tableNumberstrs)

        gameTaskSSHStrArrMap[gameName].append(sshStrsInit.replace("[:GameName]", gameName).replace("[:Parameter]", parameterString))

    return gameTaskSSHStrArrMap


if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # main()
    main({"makedate": ["2021-11-18", "2021-11-21"]})
    # main({"makedate": ["2020-01-01"], "gamename": ["lineageglobal"], "tableNumberArr": ["1001", "1002"]})
    # main({"dateRangeArr": [['2021-06-22', '2021-06-24'], ['2021-06-28', '2021-07-03']], "gamename": ["lineageglobal"], "tableNumberArr": ["11131", "1131", "1134", "1135", "1136", "1137"]})
    # main({"startdate": ["2019-12-01"], "enddate": ["2020-12-31"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
