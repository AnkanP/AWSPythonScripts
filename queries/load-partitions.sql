ALTER TABLE `raw`.`persons-incremental`
ADD PARTITION (partition_date='2023-08-19') 
location 's3://crawlerpathena/cdcload/2023-08-19/'
