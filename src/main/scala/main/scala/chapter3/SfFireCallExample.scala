package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, countDistinct, desc, to_timestamp, year}

object SfFireCallExample {

    val DATA_PATH = """C:\tools\spark\spark-3.1.1-bin-hadoop2.7\jobSpace\data\"""

    def main(args: Array[String]): Unit = {
        // In Scala

        val fireSchema = StructType(Array(
            StructField("CallNumber", IntegerType, true),
            StructField("UnitID", StringType, true),
            StructField("IncidentNumber", IntegerType, true),
            StructField("CallType", StringType, true),
            StructField("CallDate", StringType, true),
            StructField("WatchDate", StringType, true),
            StructField("CallFinalDisposition", StringType, true),
            StructField("AvailableDtTm", StringType, true),
            StructField("Address", StringType, true),
            StructField("City", StringType, true),
            StructField("Zipcode", IntegerType, true),
            StructField("Battalion", StringType, true),
            StructField("StationArea", StringType, true),
            StructField("Box", StringType, true),
            StructField("OriginalPriority", StringType, true),
            StructField("Priority", StringType, true),
            StructField("FinalPriority", IntegerType, true),
            StructField("ALSUnit", BooleanType, true),
            StructField("CallTypeGroup", StringType, true),
            StructField("NumAlarms", IntegerType, true),
            StructField("UnitType", StringType, true),
            StructField("UnitSequenceInCallDispatch", IntegerType, true),
            StructField("FirePreventionDistrict", StringType, true),
            StructField("SupervisorDistrict", StringType, true),
            StructField("Neighborhood", StringType, true),
            StructField("Location", StringType, true),
            StructField("RowID", StringType, true),
            StructField("Delay", FloatType, true)))

        val spark = SparkSession
            .builder
            .appName("SfFireCallExample")
            .getOrCreate()
        import spark.implicits._

        // Read the file using the CSV DataFrameReader
        val sfFireFile = DATA_PATH + "sf-fire-calls.csv"
        val fireDF = spark.read.schema(fireSchema)
            .option("header", "true")
            .csv(sfFireFile)

        //val parquetPath = """sfFireFile"""
        //fireDF.write.format("parquet").saveAsTable(parquetPath)

        // In Scala
        val fewFireDF = fireDF
            .select("IncidentNumber", "AvailableDtTm", "CallType")
            .where($"CallType" =!= "Medical Incident")
        fewFireDF.show(5, false)

        fireDF
            .select("CallType")
            .where(col("CallType").isNotNull)
            .agg(countDistinct('CallType) as 'DistinctCallTypes)
            .show()

        val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
        newFireDF
            .select("ResponseDelayedinMins")
            .where($"ResponseDelayedinMins" > 5)
            .show(5, false)

        val fireTsDF = newFireDF
            .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
            .drop("CallDate")
            .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
            .drop("WatchDate")
            .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
                "MM/dd/yyyy hh:mm:ss a"))
            .drop("AvailableDtTm")

        // Select the converted columns
        fireTsDF
            .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
            .show(5, false)
        // In Scala
        fireTsDF
            .select(year($"IncidentDate"))
            .distinct()
            .orderBy(year($"IncidentDate"))
            .show()

        fireTsDF
            .select("CallType")
            .where(col("CallType").isNotNull)
            .groupBy("CallType")
            .count()
            .orderBy(desc("count"))
            .show(10, false)

        import org.apache.spark.sql.{functions => F}
        fireTsDF
            .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
                F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
            .show()

    }
}
