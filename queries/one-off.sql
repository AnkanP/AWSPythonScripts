insert into "rawstd"."iceberg_table" 
select op,"commitdate","committimestamp","personid","lastname","firstname","address","city", "commitdate" 
from "raw"."persons-fullload"
