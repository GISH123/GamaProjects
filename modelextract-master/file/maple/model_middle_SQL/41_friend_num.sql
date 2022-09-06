-- Maple Modeling Middle Data : 41_friend_num
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT characterid,
    COUNT(*) AS friendNum
FROM  maple_extract.gamedb_friend
WHERE dt = '[:Date0]'
    and world = '[:gw]'
GROUP BY characterid;
