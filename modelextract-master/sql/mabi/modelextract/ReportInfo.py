
class ReportInfo():
    def __init__(self):
        self.serverCode = "600309"
        self.worldNameArr = ["01", "02", "03"]
        self.tableNumberArr = [
            # 平台資訊 -------------------------------------------------------------------------------------------------
            "1001", "1101", "6002"
            , "6006", "6007"
            # Login + -------------------------------------------------------------------------------------------------
            , "11001", "16001", "16002"
            , "16003"
            , "1002", "1003", "1102", "1103", "1804", "6019"
            , "1131", "1132", "1133", "6011", "6012"
            , "1134", "1135", "1136", "1137"
            # All Data ------------------------------------------------------------------------------------------------
            # Model 相關------------------------------------------------------------------------------------------------
        ]
        self.layerArr = [
            # 平台資訊 -------------------------------------------------------------------------------------------------
            "001", "002"
            # Login + -------------------------------------------------------------------------------------------------
            , "101", "102", "103", "104", "105"
            # All Data ------------------------------------------------------------------------------------------------
            # , "111", "112", "113"
            # Model 相關------------------------------------------------------------------------------------------------
            # , "201"
        ]
        self.layerInfoArrMap = {
            # 平台資訊 -------------------------------------------------------------------------------------------------
            "001": ["1001", "1101", "6002"]
            , "002": ["6006", "6007"]
            # Login + -------------------------------------------------------------------------------------------------
            , "101": ["11001", "16001", "16002"]
            , "102": ["16003"]
            , "103": ["1002", "1003", "1102", "1103", "1804", "6019"]
            , "104": ["1131", "1132", "1133", "6011", "6012"]
            , "105": ["1134", "1135", "1136", "1137"]
            # All Data ------------------------------------------------------------------------------------------------
            # Model 相關------------------------------------------------------------------------------------------------
        }
        self.tableNumberInfoMap = {
            "6002": {"interDependence": ["6006", "6007"]}
        }
