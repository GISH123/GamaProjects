def createTempTableSQL(tablename,path):
    sql =  f"CREATE EXTERNAL TABLE IF NOT EXISTS gtwpd.temp_maple_{tablename} ( content string)" \
           f"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' " \
           f"LOCATION '{path}' "
    print(sql)
    return sql

def countTempTableSQL(tablename):
    sql =  f"SELECT count(*) AS cnt FROM gtwpd.temp_maple_{tablename}"
    print(sql)
    return sql

def dropTempTableSQL(tablename):
    sql = f"DROP TABLE IF EXISTS gtwpd.temp_maple_{tablename}"
    print(sql)
    return sql

def inserLogSQL(tableName, read_time, row_count, dt, haveData):
    sql = f"INSERT INTO otherdata.check_maple_login " \
          f"VALUES ('{tableName}', '{read_time}', {row_count}, '{dt}', {haveData})"

    return sql

def checkLastData(tableName, datetime, limit = 3):
    sql = f"SELECT row_count, count(*) AS cnt FROM(	SELECT * FROM otherdata.check_maple_login" \
          f"WHERE tablename = '{tableName}' AND dt = '{datetime}' ORDER BY read_time DESC LIMIT {limit} )AA" \
          f"GROUP BY row_count"
    return sql



