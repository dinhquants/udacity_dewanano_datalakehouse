CREATE EXTERNAL TABLE `stedi`.`machine_learning_curated`(
  `serialnumber` string COMMENT 'from deserializer', 
  `distanceFromObject` bigint COMMENT 'from deserializer', 
  `sensorReadingTime` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ('paths'='birthDay,customerName,email,lastUpdateDate,phone,registrationDate,serialNumber,shareWithPublicAsOfDate,shareWithResearchAsOfDate') 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://dinh-nda/step_trainer/curated/'
TBLPROPERTIES ('classification'='json')