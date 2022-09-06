
class ReportInfo() :
    def __init__(self):
        self.serverCode = "000000"
        self.worldNameArr = [
            "101"
        ]
        self.tableNumberArr = [
            "1001", "1002", "1003"
            , "1101", "1102", "1103"
            , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
            , "1802", "1804"
            , "6001", "6002", "6019"
            , "6011", "6012"
            , "11802", "11803"
            , "16001" , "16002"
        ]
        self.layerArr = ["101","102", "103", "104","121","122","131","132","133","134"]
        self.layerInfoArrMap = {
            "101" : []
            , "102" : ["1001", "1002", "1003", "1101", "1102", "1103"]
            , "103" : ["1131", "1132", "1133"]
            , "104" : ["1134", "1135", "1136", "1137"]
            , "121" : ["11802", "11803" ]
            , "122" : ["1802", "1804" ]
            , "131": ["16001","16002"]
            , "132": []
            , "133": ["6001","6002","6019"]
            , "134": ["6011","6012"]
        }

        self.tableNumberInfoMap = {
            "11802": {"interDependence": ["1802", "1804"] }
            , "16001": {"interDependence": ["6001"]}
            , "16002": {"interDependence": ["6002", "6019"]}
            , "1103": {"interDependence": ["1131", "1132", "1133"]}
            , "1131": {"interDependence": ["1134", "1135", "1136", "1137"]}
        }

        self.worldNameArr_old_20201001_to_20210427 = [
            "101"
            , "102"
            , "103"
            , "104"
            , "105"
            , "106"
            , "1011"
        ]
