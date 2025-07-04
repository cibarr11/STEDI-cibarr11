CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing (
  sensorReadingTime BIGINT,
  serialNumber STRING,
  distanceFromObject FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3://stedi-project-data-cei/step_trainer_landing/';