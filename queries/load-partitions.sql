ALTER TABLE `demo`.`persons-incremental`
ADD PARTITION (partition_date='2023-08-04') 
location 's3://crawlerpathena/cdcload/2023-08-04/'
