package main.scala.chapter3

import org.apache.spark.sql.SparkSession

object DataSetsExample extends App {

    val spark = SparkSession
        .builder
        .appName("DataSetsExample")
        .config("spark.master", "local")
        .getOrCreate()

    import spark.sqlContext.implicits._

    val ds = spark
        .read
        .json("C:/tools/spark/spark-3.1.1-bin-hadoop2.7/jobSpace/data/iot_devices.json")
        .as[DeviceIOTData]

    ds.show(5, false)

    val filterTempDS = ds
        .filter(d => d.temp > 30 && d.humidity > 70)

    val filterDS = ds
        .select($"temp", $"device_name", $"device_id", $"cca3")
        .where("temp > 25")

    filterTempDS.show(5, false)
    filterDS.show(5, false)

    val filterTempDS2 = ds.filter({d => {d.temp > 30 && d.humidity > 70}})
    filterTempDS2.show()

}

// Easiest way to define a schema is using case class. Its like java bean
case class DeviceIOTData(battery_level: Long, c02_level: Long, cca2: String,
                         cca3: String, cn: String, device_id: Long,
                         device_name: String, humidity: Long, ip: String,
                         latitude: Double, lcd: String, longitude: Double,
                         scale: String, temp: Long, timestamp: Long)
