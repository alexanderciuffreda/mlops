
  .addColumn(,"INT"firstName,"INT", ,"INT"STRING,"INT") \
  .addColumn(,"INT"middleName,"INT", ,"INT"STRING,"INT") \
  .addColumn(,"INT"lastName,"INT", ,"INT"STRING,"INT", comment = ,"INT"surname,"INT") \
  .addColumn(,"INT"gender,"INT", ,"INT"STRING,"INT") \
  .addColumn(,"INT"birthDate,"INT", ,"INT"TIMESTAMP,"INT") \
  .addColumn(,"INT"ssn,"INT", ,"INT"STRING,"INT") \
  .addColumn(,"INT"salary,"INT", ,"INT"INT,"INT") \
  .property(,"INT"description,"INT", ,"INT"table with people data,"INT") \
  .location(,"INT"/tmp/delta/people10m,"INT") \
  .execute()

  myCSV= spark.read.csv(,"INT"/path/to/input/data,"INT",header=True,sep=,"INT",,"INT"); 
DeltaTable.createOrReplace(spark) \
.tableName("test_data")\
.addColumn("id","INT") \
.addColumn("cycle","INT")\
.addColumn("op1","INT") \
.addColumn("op2","INT") \
.addColumn("op3","INT") \
.addColumn("sensor1","INT") \
.addColumn("sensor2","INT") \
.addColumn("sensor3","INT") \
.addColumn("sensor4","INT") \
.addColumn("sensor5","INT") \
.addColumn("sensor6","INT") \
.addColumn("sensor7","INT") \
.addColumn("sensor8","INT") \
.addColumn("sensor9","INT") \
.addColumn("sensor10","INT") \
.addColumn("sensor11","INT") \
.addColumn("sensor12","INT") \
.addColumn("sensor13","INT") \
.addColumn("sensor14","INT") \
.addColumn("sensor15","INT") \
.addColumn("sensor16","INT") \
.addColumn("sensor17","INT") \
.addColumn("sensor18","INT") \
.addColumn("sensor19","INT") \
.addColumn("sensor20","INT") \
.addColumn("sensor21","INT") \
.addColumn("sensor22","INT") \
.addColumn("sensor23","INT") \
.execute()