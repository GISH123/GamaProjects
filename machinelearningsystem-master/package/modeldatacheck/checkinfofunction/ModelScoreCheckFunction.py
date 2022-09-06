from package.modeldatacheck.checkinfofunction.CommonCheckFunction import CommonCheckFunction


class ModelScoreCheckFunction(CommonCheckFunction) :

    def ModelScoreCheck_isnotnull(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnull(columnInfo)
        return mapColumn, mainColumn

    def ModelScoreCheck_isnotnullper(self,columnInfo):
        mapColumn, mainColumn = self.CommonCheck_isnotnullper(columnInfo)
        return mapColumn , mainColumn

    def ModelScoreCheck_percentile(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_percentile(columnInfo)
        return mapColumn, mainColumn

    def ModelScoreCheck_avg(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_avg(columnInfo)
        return mapColumn, mainColumn

    def ModelScoreCheck_round(self, columnInfo):
        mapColumn, mainColumn = self.CommonCheck_round(columnInfo)
        return mapColumn, mainColumn


