CREATE EXTERNAL TABLE IF NOT EXISTS accelerometer_landing(
  user string,
  `timestamp` bigint,
  x float,
  y float,
  z float
  )
LOCATION 's3://my-bucket-of-glue/accelerometer/landing/'