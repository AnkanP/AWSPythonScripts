ALTER TABLE `raw`.`persons-incremental`
ADD PARTITION (partition_date='<REPLACEDATE>') 
location 's3://crawlerpathena/cdcload/<REPLACEDATE>/'
