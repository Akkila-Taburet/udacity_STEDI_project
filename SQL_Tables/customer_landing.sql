CREATE EXTERNAL TABLE IF NOT EXISTS `akkila-stedi`.`customer_landing` (
  `serialnumber` string,
  `shareWithPublicAsOfDate` bigint,
  `birthday` string,
  `registrationDate` bigint,
  `shareWithResearchAsOfDate` bigint,
  `customerName` string,
  `email` string,
  `lastUpdateDate` bigint,
  `phone` string,
  `shareWithFriendsAsOfDate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://udacity-stedi-akkila/customer/landing/'
TBLPROPERTIES (
  'classification' = 'json',
  'write.compression' = 'NONE'
);
