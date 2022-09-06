import os , sys, datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
import maple.P00_MakeLogin as P00Ctrl
import maple.P10_MakePartition as P10Ctrl
import maple.P20_MakeExtract as P20Ctrl
import maple.P30_MakeMiddle as P30Ctrl
import maple.P50_MakeALLMonthFirst as P50Ctrl
import maple.P70_UseModel as P70Ctrl


def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]

    makeDateStrArr = [(nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
        ,(nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")] if makeDateStrArr == [] else makeDateStrArr

    for makeDateStr in makeDateStrArr :
        mainALLData(makeDateStr)

def mainALLData(makeTimeStr = ""):
    ################################################################################################
    runP00 = False
    runP10 = False
    runP20 = False
    runP30 = True
    runP50 = False
    runP70 = True

    ####################################################################################################
    run_parametersData = {
        "startdate": [makeTimeStr]
        , "enddate": [makeTimeStr]
    }
    """
        runP00
    """
    if runP00 == True :
        P00Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP10
    """
    run_parametersData_partition = {
        "startdate": [makeTimeStr]
        , "enddate": [(datetime.datetime.strptime(makeTimeStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
    }
    if runP10 == True:
        P10Ctrl.main(run_parametersData_partition)
    ####################################################################################################
    """
        runP20
    """
    if runP20 == True:
        P20Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP30
    """
    if runP30 == True:
        P30Ctrl.main(run_parametersData)
    ####################################################################################################
    """
        runP50
    """
    if runP50 == True:
        P50Ctrl.main()
    ####################################################################################################
    """
        runP70
    """
    if runP70 == True:
        try:
            P70Ctrl.main(run_parametersData)
        except:
            print('Warning: model build fail')
if __name__ == "__main__" :
    main({"makedate": ["2021-04-14"]})