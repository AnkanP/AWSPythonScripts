MERGE INTO "rawstd"."iceberg_table" t
USING (	
    SELECT op,"commitdate","committimestamp","personid","lastname","firstname","address","city", "partition_date" as "partition_date"
    FROM "raw"."persons-incremental" t1
    JOIN (SELECT "personid" "id", max("committimestamp") "stc" 
        FROM "raw"."persons-incremental" 
            WHERE partition_date ='<REPLACEDATE>'
                GROUP BY "personid") t2
    ON t1."personid" = t2."id" AND t1."committimestamp" = t2."stc"
    WHERE t1.partition_date ='<REPLACEDATE>') s
ON t."personid" = s."personid"
WHEN MATCHED AND s.op = '"D"' THEN DELETE
WHEN MATCHED THEN
UPDATE SET
"op" = s.op,
"commitdate" = s."commitdate",
"committimestamp" = s."committimestamp",
"personid" = s."personid",
"lastname" = s."lastname",
"firstname" = s."firstname",
"address" = s."address",
"city"  = s."city",
"partition_date" = cast(s."partition_date" as date)
WHEN NOT MATCHED AND s.op != '"D"' THEN
INSERT (op,"commitdate","committimestamp","personid","lastname","firstname","address","city", "partition_date")
VALUES
(s."op",
s."commitdate",
s."committimestamp",
s."personid",
s."lastname",
s."firstname",
s."address",
s."city",
cast(s."partition_date" as date))
