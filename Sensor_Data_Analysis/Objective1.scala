package Sensor_Data_Analysis

import org.apache.spark.sql.SparkSession

object Objective1
{

  def main(args: Array[String]): Unit =

  {
    println("Sensor data analysis!!!")

    //create a spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Working with Sensor Data")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //load the dataset using the csvFile method
    val hvac_data = spark
      .read.format("com.databricks.spark.csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\CaseStudies\\CaseStudy3SensorCaseStudy\\Dataset\\HVAC.csv")

    //converting the dataset RDD into dataframe
    val hvac_data_df = hvac_data.toDF

    // loading hvac dataframe into temporary table 'hvacTempTable'
    hvac_data_df.registerTempTable("hvacTempTable")

    println("Dataframe Registered as Table !")

    //Use spark-sql to query the hvacTempTable and adding an extra column which gives temprature change result

    val hvac_temp_chnage = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from hvacTempTable")

    hvac_temp_chnage.show()
  }
}
