CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing(
  customername string,
  email string,
  phone string,
  birthday string,
  serialnumber string,
  registrationdate bigint,
  lastupdatedate bigint,
  sharewithresearchasofdate bigint,
  sharewithpublicasofdate bigint,
  sharewithfriendsasofdate bigint
  )
LOCATION 's3://my-bucket-of-glue/customer/landing/'