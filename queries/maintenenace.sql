#show partitions:
SELECT * FROM "demo"."iceberg_table$partitions" 

#show snapshots
SELECT * FROM "demo"."iceberg_table$snapshots" 

#show history
SELECT * FROM "demo"."iceberg_table$history"

#time travel queries



#version travel queries

#vaccum
ALTER TABLE iceberg_table SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='259200'
)

ALTER TABLE demo.iceberg_table SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='1',
  'vacuum_min_snapshots_to_keep'='2'
)