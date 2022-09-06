
class CommonCheckFunction():

    def CommonCheck_isnotnull(self,columnInfo):
        name = columnInfo['name']
        mapColumn = "'{}_isnotnull', AAA.{}_isnotnull".format(name, name)
        if columnInfo['datatype'] == 'string':
            mainColumn = "SUM( CASE WHEN AA.{} IS NULL THEN 0 WHEN AA.{} = {} THEN 0 ELSE 1 END) AS {}_isnotnull ".format(name, name, "''", name)
        else:
            mainColumn = "SUM( CASE WHEN AA.{} IS NULL THEN 0 WHEN AA.{} = {} THEN 0 ELSE 1 END) AS {}_isnotnull ".format(name, name, "0", name)
        return mapColumn , mainColumn

    def CommonCheck_isnotnullper(self,columnInfo):
        name = columnInfo['name']
        mapColumn = "'{}_isnotnullper', AAA.{}_isnotnullper".format(name, name)
        if columnInfo['datatype'] == 'string':
            mainColumn = "SUM(CASE WHEN AA.{} IS NULL THEN 0 WHEN AA.{} = {} THEN 0 ELSE 1 END)/SUM(1) AS {}_isnotnullper ".format(name, name, "''", name)
        else:
            mainColumn = "SUM(CASE WHEN AA.{} IS NULL THEN 0 WHEN AA.{} = {} THEN 0 ELSE 1 END)/SUM(1) AS {}_isnotnullper ".format(name, name, "0", name)
        return mapColumn , mainColumn

    def CommonCheck_percentile(self,columnInfo):
        name = columnInfo['name']
        mapColumn = "'{}_percentile', AAA.{}_percentile".format(name, name)
        mainColumn = "percentile(cast(AA.{} as bigint),0.5) AS {}_percentile ".format(name, name)
        return mapColumn , mainColumn

    def CommonCheck_avg(self,columnInfo):
        name = columnInfo['name']
        mapColumn = "'{}_avg', AAA.{}_avg".format(name, name)
        mainColumn = "avg(AA.{}) AS {}_avg ".format(name, name)
        return mapColumn , mainColumn

    def CommonCheck_round(self,columnInfo):
        name = columnInfo['name']
        mapColumn = "'{}_round', AAA.{}_round".format(name, name)
        mainColumn = "Max(AA.{}) - Min(AA.{}) AS {}_round ".format(name, name, name)
        return mapColumn , mainColumn


