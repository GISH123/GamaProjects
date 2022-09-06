-- Maple Modeling Middle Data: 42_album_num
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    characterid,
    COUNT(*) AS albumsize
FROM
     maple_extract.gamedb_beautyalbum
WHERE
    dt = '[:Date0]'
    and world = '[:gw]'
GROUP BY characterid;
