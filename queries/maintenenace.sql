#show partitions:
SELECT * FROM "rawstd"."iceberg_table$partitions" 

#show snapshots
SELECT * FROM "rawstd"."iceberg_table$snapshots" 

#show history
SELECT * FROM "rawstd"."iceberg_table$history"

#time travel queries



#version travel queries

#vaccum
ALTER TABLE rawstd.iceberg_table SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='259200'
)

ALTER TABLE rawstd.iceberg_table SET TBLPROPERTIES (
  'vacuum_max_snapshot_age_seconds'='1',
  'vacuum_min_snapshots_to_keep'='2'
)