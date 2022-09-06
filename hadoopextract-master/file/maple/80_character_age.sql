-- Maple Modeling Middle Data
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    SELECT
        sn ,
        gameworldid,
        characterid,
        charactername,
        accountid,
        registerdate

    FROM
        maple_all.globalaccount_character
    WHERE
        dt = '[:LastDate]'
        AND gameworldid = [:gwid]
        AND characterid % 10 = [:cid_key]
        ;


