package Sensor_Data_Analysis

import org.apache.spark.sql.SparkSession

object Objective3
 {

   case class HVAC(Date:String,Time:String,TargetTemp:Int,ActualTemp:Int,System:Int,SystemAge:Int,BuildingID:Int)
   
   case class BUILDING(BuildingID:Int,BuildingMgr:String,BuildingAge:Int,HVAC_Product:String,Country:String)

   def main(args: Array[String]): Unit =

   {
     println("Sensor data analysis!!!")

     val spark = SparkSession
       .builder()
       .master("local")
       .appName("Working with Sensor Data")
       .config("spark.some.config.option", "some-value")
       .getOrCreate()

     println("Spark Session is created !!!")


     val hvac_data_with_header = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\CaseStudies\\CaseStudy3SensorCaseStudy\\Dataset\\HVAC.csv")

     val header = hvac_data_with_header.first()

     val hvac_data = hvac_data_with_header.filter(row => row != header)

     import spark.implicits._

     val hvac_data_df = hvac_data.map(x=>x.split(",")).map(x => HVAC(x(0),x(1),x(2).toInt,x(3).toInt,x(4).toInt,x(5).toInt,x(6).toInt)).toDF()

     hvac_data_df.registerTempTable("hvacTempTable")

     val hvac_1 = spark.sql("select *,IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS tempchange from hvacTempTable")

     hvac_1.registerTempTable("hvac1TempTable")

     println("Data Frame Registered as hvac1TempTable table !")




     val building_data_with_header = spark.sparkContext.textFile("C:\\Users\\Bhaskar\\Desktop\\AcadGild\\CaseStudies\\CaseStudy3SensorCaseStudy\\Dataset\\building.csv")


     val header1 = building_data_with_header.first()

     val building_data = building_data_with_header.filter(row => row != header1)

     val building_data_df = building_data.map(x=> x.split(",")).map(x => BUILDING(x(0).toInt,x(1),x(2).toInt,x(3),x(4))).toDF

     building_data_df.registerTempTable("buildingTempTable")


     val join_hvac_building = spark.sql("select h.*, b.Country, b.HVAC_product from buildingTempTable b join hvac1TempTable h on b.BuildingID = h.BuildingID")

     join_hvac_building.show()
     

     val tempCountry = join_hvac_building.map(x => (new Integer(x(7).toString),x(8).toString))

     tempCountry.withColumnRenamed("_1","tempchange").withColumnRenamed("_2","country")show()

     

     val tempCountryOnes = tempCountry.filter(x=> {if(x._1==1) true else false})


     tempCountryOnes.groupBy("_2").count.withColumnRenamed("_2","country" )show()


   }
 }
