CREATE TABLE rawstd.iceberg_table (
 op string, 
  commitdate date, 
  committimestamp timestamp, 
  personid int, 
  lastname string, 
  firstname string, 
  address string, 
  city string,
  partition_date date)
PARTITIONED BY (`partition_date`, bucket(16, personid))
LOCATION 's3://crawlerpathena/iceberg/'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='ZSTD',
  'format'='PARQUET'
);