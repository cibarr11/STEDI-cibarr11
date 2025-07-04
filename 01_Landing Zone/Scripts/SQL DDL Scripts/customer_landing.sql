CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customerName STRING,
  email STRING,
  phone STRING,
  birthDay STRING,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://stedi-project-data-cei/customer_landing/';