CREATE EXTERNAL TABLE `raw.persons-incremental`(
  `op` string, 
  `commitdate` date, 
  `committimestamp` timestamp, 
  `personid` bigint, 
  `lastname` string, 
  `firstname` string, 
  `address` string, 
  `city` string)
PARTITIONED BY ( 
  `partition_date` string)
  ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://crawlerpathena/cdcload'

