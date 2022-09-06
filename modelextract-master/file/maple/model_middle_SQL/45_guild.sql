 -- Maple Modeling Middle Data:45_guild
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    characterid,
    MAX(guildid) AS guildid,
    MAX(grade) AS grade,
    MAX(commitment_i) AS commitment_i,
    MAX(commitment_f) AS commitment_f,
    MAX(commitment_f) - MAX(commitment_i) AS commitment_diff
FROM
(
    SELECT
        characterid,
        guildid,
        grade,
        0 AS commitment_i,
        commitment AS commitment_f
    FROM  maple_extract.gamedb_guildmember
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
    UNION ALL
    SELECT
        characterid,
        guildid,
        grade,
        commitment AS commitment_i,
        0 AS commitment_f
    FROM  maple_extract.gamedb_guildmember
    WHERE
        dt = '[:DateP]'
        AND world = '[:gw]'
)AA
GROUP BY characterid
HAVING commitment_i != commitment_f
;

