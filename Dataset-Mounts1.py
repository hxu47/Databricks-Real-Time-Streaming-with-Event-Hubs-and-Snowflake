# Databricks notebook source
# Mount Blog Storage to databricks environment 

# COMMAND ----------

# MAGIC %scala
# MAGIC def retryMount(source: String, mountPoint: String): Unit = {
# MAGIC   try { 
# MAGIC     // Mount with IAM roles instead of keys for PVC
# MAGIC     dbutils.fs.mount(source, mountPoint)
# MAGIC   } catch {
# MAGIC     case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def mountFailed(msg:String): Unit = {
# MAGIC   println(msg)
# MAGIC }
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
# MAGIC   try {
# MAGIC     dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
# MAGIC   } catch {
# MAGIC     case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
# MAGIC     case e: Exception => mountFailed(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}")
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC def mountSource(mountDir:String, source:String, extraConfigs:Map[String,String]): String = {
# MAGIC   val mntSource = source
# MAGIC
# MAGIC   if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
# MAGIC     val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
# MAGIC     if (mount.source == mntSource) {
# MAGIC       return s"""Datasets are already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
# MAGIC       
# MAGIC     }
# MAGIC       else{
# MAGIC      return "Invalid Mounts"
# MAGIC       } 
# MAGIC     }
# MAGIC   else {
# MAGIC     println(s"""Mounting datasets to $mountDir from $mntSource""")
# MAGIC     mount(source, extraConfigs, mountDir)
# MAGIC     return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
# MAGIC   }
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC def getAzureRegion():String = {
# MAGIC   import com.databricks.backend.common.util.Project
# MAGIC   import com.databricks.conf.trusted.ProjectConf
# MAGIC   import com.databricks.backend.daemon.driver.DriverConf
# MAGIC
# MAGIC   new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
# MAGIC   var MAPPINGS = Map(
# MAGIC     "eastus"    -> ("dbtraineastus",
# MAGIC                               "?ss=b&sp=rl&sv=2018-03-28&st=2018-04-01T00%3A00%3A00Z&sig=tlw5PMp1DMeyyBGTgZwTbA0IJjEm83TcCAu08jCnZUo%3D&srt=sco&se=2023-04-01T00%3A00%3A00Z"))
# MAGIC   
# MAGIC
# MAGIC
# MAGIC   val (account: String, sasKey: String) = MAPPINGS.getOrElse(azureRegion, MAPPINGS("_default"))
# MAGIC   val blob = "training"
# MAGIC   val source = s"wasbs://$blob@$account.blob.core.windows.net/"
# MAGIC   val config = Map(
# MAGIC     s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
# MAGIC   )
# MAGIC   val (sasEntity, sasToken) = config.head
# MAGIC
# MAGIC   val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
# MAGIC   spark.conf.set("com.databricks.training.azure.datasource", datasource)
# MAGIC
# MAGIC   return (source,config)
# MAGIC }

# COMMAND ----------

# MAGIC %scala
# MAGIC def autoMount(mountDir:String = "/mnt/training"): Unit = {
# MAGIC     val azureRegion = getAzureRegion()
# MAGIC     print(azureRegion+"\n")
# MAGIC     spark.conf.set("com.databricks.training.region.name", azureRegion)
# MAGIC     val (source, extraConfigs) =initAzureDataSource(azureRegion)  
# MAGIC   val resultMsg = mountSource( mountDir, source, extraConfigs)
# MAGIC   displayHTML(resultMsg)
# MAGIC }
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC autoMount()
