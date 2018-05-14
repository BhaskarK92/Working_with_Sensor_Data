package Sensor_Data_Analysis

import org.apache.spark.sql.SparkSession

object Objective2
{
  def main(args: Array[String]): Unit =

  {

    println("Sensor data analysis!!!")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Working with Sensor Data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val building_data = spark
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\CaseStudies\\CaseStudy3SensorCaseStudy\\Dataset\\building.csv")

    val building_data_df =building_data.toDF

    building_data_df.registerTempTable("buildingTempTable")

    val load = spark.sql("select * from buildingTempTable").show()

  }

}
