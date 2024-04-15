CREATE EXTERNAL TABLE IF NOT EXISTS step_trainer_landing(
    sensorReadingTime bigint,
    serialNumber string,
    distanceFromObject int
) LOCATION 's3://my-bucket-of-glue/step_trainer/landing/'