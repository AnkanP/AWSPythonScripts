insert into "demo"."iceberg_table" 
select op,"commitdate","committimestamp","personid","lastname","firstname","address","city", "commitdate" 
from "demo"."persons-fullload"