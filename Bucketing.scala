// Databricks notebook source
// MAGIC %sql show create table default.salaries_temp

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE `default`.`salaries_non_bucketed_1` ( `Id` INT, `EmployeeName` STRING, `JobTitle` STRING, `BasePay` STRING, `OvertimePay` STRING, `OtherPay` STRING, `Benefits` STRING, `TotalPay` DOUBLE, `TotalPayBenefits` DOUBLE, `Year` INT, `Notes` STRING, `Agency` STRING, `Status` STRING) USING csv OPTIONS ( `multiLine` 'false', `escape` '"', `header` 'true', `delimiter` ',')  LOCATION 'dbfs:/FileStore/tables/salaries_non_bucketed_1'

// COMMAND ----------

// MAGIC %sql insert into `default`.`salaries_non_bucketed_1` select * from default.salaries_temp

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE `default`.`salaries_non_bucketed_2` ( `Id` INT, `EmployeeName` STRING, `JobTitle` STRING, `BasePay` STRING, `OvertimePay` STRING, `OtherPay` STRING, `Benefits` STRING, `TotalPay` DOUBLE, `TotalPayBenefits` DOUBLE, `Year` INT, `Notes` STRING, `Agency` STRING, `Status` STRING) USING csv OPTIONS ( `multiLine` 'false', `escape` '"', `header` 'true', `delimiter` ',')  LOCATION 'dbfs:/FileStore/tables/salaries_non_bucketed_2'

// COMMAND ----------

// MAGIC %sql insert into `default`.`salaries_non_bucketed_2` select * from default.salaries_temp

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE `default`.`salaries_1` ( `Id` INT, `EmployeeName` STRING, `JobTitle` STRING, `BasePay` STRING, `OvertimePay` STRING, `OtherPay` STRING, `Benefits` STRING, `TotalPay` DOUBLE, `TotalPayBenefits` DOUBLE, `Year` INT, `Notes` STRING, `Agency` STRING, `Status` STRING) USING csv OPTIONS ( `multiLine` 'false', `escape` '"', `header` 'true', `delimiter` ',')  clustered by (Id) sorted by (Id) into 1024 buckets LOCATION 'dbfs:/FileStore/tables/salaries_1'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE `default`.`salaries_2` ( `Id` INT, `EmployeeName` STRING, `JobTitle` STRING, `BasePay` STRING, `OvertimePay` STRING, `OtherPay` STRING, `Benefits` STRING, `TotalPay` DOUBLE, `TotalPayBenefits` DOUBLE, `Year` INT, `Notes` STRING, `Agency` STRING, `Status` STRING) USING csv OPTIONS ( `multiLine` 'false', `escape` '"', `header` 'true', `delimiter` ',')  clustered by (Id) sorted by (Id) into 1024 buckets LOCATION 'dbfs:/FileStore/tables/salaries_2'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE `default`.`salaries_3` ( `Id` INT, `EmployeeName` STRING, `JobTitle` STRING, `BasePay` STRING, `OvertimePay` STRING, `OtherPay` STRING, `Benefits` STRING, `TotalPay` DOUBLE, `TotalPayBenefits` DOUBLE, `Year` INT, `Notes` STRING, `Agency` STRING, `Status` STRING) USING csv OPTIONS ( `multiLine` 'false', `escape` '"', `header` 'true', `delimiter` ',')  clustered by (Id) sorted by (Id) into 1500 buckets LOCATION 'dbfs:/FileStore/tables/salaries_3'

// COMMAND ----------

// MAGIC %sql insert into `default`.`salaries_1` select * from default.salaries_temp

// COMMAND ----------

// MAGIC %sql insert into `default`.`salaries_2` select * from default.salaries_temp

// COMMAND ----------

// MAGIC %sql insert into `default`.`salaries_3` select * from default.salaries_temp

// COMMAND ----------

val path="dbfs:/FileStore/tables/salaries_1"
val filelist=dbutils.fs.ls(path)
val df = filelist.toDF()
println("Number of files:"+df.count())

// get the size
//df.createOrReplaceTempView("adlsSize")
//spark.sql("select sum(size)/(1024*1024*1024) as sizeInGB from adlsSize").show()

// COMMAND ----------

val path="dbfs:/FileStore/tables/salaries_2"
val filelist=dbutils.fs.ls(path)
val df = filelist.toDF()
println("Number of files:"+df.count())

// get the size
//df.createOrReplaceTempView("adlsSize")
//spark.sql("select sum(size)/(1024*1024*1024) as sizeInGB from adlsSize").show()

// COMMAND ----------

val path="dbfs:/FileStore/tables/salaries_3"
val filelist=dbutils.fs.ls(path)
val df = filelist.toDF()
println("Number of files:"+df.count())

// get the size
//df.createOrReplaceTempView("adlsSize")
//spark.sql("select sum(size)/(1024*1024*1024) as sizeInGB from adlsSize").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.salaries_1 a
// MAGIC inner join default.salaries_2 b
// MAGIC on(a.Id=b.Id)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.salaries_1 a
// MAGIC inner join default.salaries_temp b
// MAGIC on(a.Id=b.Id)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.salaries_1 a
// MAGIC inner join default.salaries_3 b
// MAGIC on(a.Id=b.Id)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.salaries_1 a
// MAGIC inner join default.salaries_2 b
// MAGIC on(a.EmployeeName=b.EmployeeName)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from default.salaries_1 a
// MAGIC inner join 
// MAGIC (select * from default.salaries_2 union select * from default.salaries_1)b
// MAGIC on(a.Id=b.Id)

// COMMAND ----------


