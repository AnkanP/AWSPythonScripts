
CREATE EXTERNAL TABLE `demo.persons-fullload`(
  `op` string, 
  `commitdate` date, 
  `committimestamp` timestamp, 
  `personid` bigint, 
  `lastname` string, 
  `firstname` string, 
  `address` string, 
  `city` string)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY ',' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://crawlerpathena/fullload'

