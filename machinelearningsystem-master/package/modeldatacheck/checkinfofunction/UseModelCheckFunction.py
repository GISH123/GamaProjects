from package.modeldatacheck.checkinfofunction.CommonCheckFunction import CommonCheckFunction


class UseModelCheckFunction(CommonCheckFunction) :

    def UseModelCheck_isnotnull(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnull(columnInfo)
        return mapColumn, mainColumn

    def UseModelCheck_isnotnullper(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnullper(columnInfo)
        return mapColumn , mainColumn

    def UseModelCheck_percentile(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_percentile(columnInfo)
        return mapColumn, mainColumn

    def UseModelCheck_avg(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_avg(columnInfo)
        return mapColumn, mainColumn

    def UseModelCheck_round(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_round(columnInfo)
        return mapColumn, mainColumn


