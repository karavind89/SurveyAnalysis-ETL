/**
  * Created by aravikri on 10/01/2016.
  */

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class ETL(sc: SparkContext){

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  def Extract(input_location: String): DataFrame ={
    val df = sqlContext.read.json(input_location)
    return df
  }

  def Transform(df: DataFrame): Unit = {
    val job1 = new ETL(sc)

    /** Measure dimensional table**/
    val measure_df = df.select($"data"(32) as "MeasureId", $"data"(37) as "SubMeasureID", $"data"(13) as "MeasureDesc",
      $"data"(16) as "Data_Value_Unit",$"data"(17) as "Data_Value_Type",$"data"(18).cast(FloatType).as("Data_Value"),
      $"data"(19) as "Data_Value_Footnote_Symbol",$"data"(20) as "Data_Value_Footnote").rdd
    //Schema to be applied to the table
    val measure_schema = (new StructType).add("MeasureId", StringType).add("SubMeasureID", StringType).add("MeasureDesc", StringType)
      .add("Data_Value_Unit", StringType).add("Data_Value_Type", StringType).add("Data_Value", FloatType)
      .add("Data_Value_Footnote_Symbol", StringType).add("Data_Value_Footnote", StringType)
    val measure_table = sqlContext.createDataFrame(measure_df, measure_schema).dropDuplicates()
    //Test case to check the data availability
    assert(measure_table.count() > 0,{measure_table.count() == 0; println("measure_table doesnt contain data")})

    job1.Load(measure_table,"measure_dimension_table")


    /** Topic dimensional table**/
    val topic_df = df.select($"data"(30) as "TopicTypeId", $"data"(31) as "TopicId",$"data"(12) as "TopicDesc",$"data"(15) as "Response",
      $"data"(38).cast(IntegerType).as("DisplayOrder"),$"data"(8) as "YEAR",$"data"(9) as "LocationAbbr",
      $"data"(10) as "LocationDesc", $"data"(14) as "DataSource").rdd
    //Schema to be applied to the table
    val topic_schema = (new StructType).add("TopicTypeId", StringType).add("TopicId", StringType).add("TopicDesc",StringType)
      .add("Response", StringType).add("DisplayOrder", IntegerType).add("YEAR", StringType)
      .add("LocationAbbr", StringType).add("LocationDesc", StringType).add("DataSource", StringType)
    val topic_table = sqlContext.createDataFrame(topic_df, topic_schema).dropDuplicates()

    assert(topic_table.count() > 0,{topic_table.count() == 0; println("topic_table doesnt contain data")})

    job1.Load(topic_table,"topic_dimension_table")

    /** Personal Information dimensional table**/
    val personal_info_df = df.select($"data"(25) as "Gender",$"data"(26) as "Race",$"data"(27) as "Age",$"data"(28) as "Education",
      $"data"(15) as "Response",$"data"(29) as "GeoLocation").rdd
    //Schema to be applied to the table
    val personal_info_schema = (new StructType).add("Gender", StringType).add("Race", StringType).add("Age", StringType).add("Education", StringType)
      .add("Response", StringType).add("GeoLocation", StringType)
    val personal_info_table = sqlContext.createDataFrame(personal_info_df,personal_info_schema).dropDuplicates()

    assert(personal_info_table.count() > 0,{personal_info_table.count() == 0; println("personal_info_table doesnt contain data")})

    job1.Load(personal_info_table,"personal_info_table")

    /** Fact Table**/
    val fact_df = df.select($"data"(30) as "TopicTypeId", $"data"(31) as "TopicId", $"data"(32) as "MeasureId",$"data"(33) as "StratificationID1",
      $"data"(34) as   "StratificationID2",$"data"(35) as "StratificationID3",$"data"(36) as "StratificationID4",
      $"data"(37) as "SubMeasureID",$"data"(21).cast(FloatType).as( "Data_Value_Std_Err"),$"data"(22).cast(FloatType).as("Low_Confidence_Limit"),
      $"data"(23).cast(FloatType).as("High_Confidence_Limit"),$"data"(24).cast(IntegerType).as("Sample_Size")).rdd
    //Schema to be applied to the table
    val fact_schema = (new StructType).add("TopicTypeId", StringType).add("TopicId", StringType).add("MeasureId", StringType).add("StratificationID1", StringType)
      .add("StratificationID2", StringType).add("StratificationID3", StringType).add("StratificationID4", StringType).add("SubMeasureID", StringType)
      .add("Data_Value_Std_Err", FloatType).add("Low_Confidence_Limit", FloatType).add("High_Confidence_Limit", FloatType)
      .add("Sample_Size",IntegerType)
    val fact_table = sqlContext.createDataFrame(fact_df, fact_schema).dropDuplicates()

    assert(fact_table.count() > 0,{fact_table.count() == 0; println("fact_table doesnt contain data")})

    job1.Load(fact_table,"fact_table")

    val result_agg = fact_table.join(topic_table,fact_table("TopicId") === topic_table("TopicId")).groupBy("YEAR").agg(
      "Data_Value_Std_Err" -> "sum",
      "Low_Confidence_Limit" -> "max",
      "High_Confidence_Limit" -> "avg",
      "Sample_Size" -> "count"
    )

    result_agg.show()


  }

  def Load(transform : DataFrame,file_name : String): Unit ={
    def pwd = System.getProperty("user.dir")
    transform.write.format("parquet").save(pwd + "\\Output\\" + file_name + ".parquet")

    //def apply_function(a: DataFrame) = a.write.format("parquet").save("C:\\Users\\aravikri\\Desktop\\CentroProjec\\Output\\" + a + ".parquet")
    //transform.productIterator.map(_.asInstanceOf[DataFrame]).foreach(a => apply_function(a))

  }
}

object SurveyAnalysis {

  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("SparkAnalysis").setMaster("local")
    val sc = new SparkContext(conf)
    val start_time = System.nanoTime()
    def pwd = System.getProperty("user.dir")
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val input_location = pwd + """\Data\tobacco_data.json"""
    val job = new ETL(sc)
    val json_to_df = job.Extract(input_location)
    val transform = job.Transform(json_to_df)
    val stop_time = System.nanoTime()

    println("Time Taken for ETL Process to complete (in seconds): " + (stop_time - start_time)/ 1000000000)



  }
}

// References
// http://beekeeperdata.com/posts/hadoop/2015/12/14/spark-scala-tutorial.html
// http://docs.scala-lang.org/tutorials/tour/classes.html
// http://teknosrc.com/spark-error-java-io-ioexception-could-not-locate-executable-null-bin-winutils-exe-hadoop-binaries/
// https://spark.apache.org/docs/1.6.0/api/scala/index.html#org.apache.spark.sql.GroupedData
// https://spark.apache.org/docs/1.4.0/api/java/org/apache/spark/sql/GroupedData.html
// https://hadoopist.wordpress.com/2016/02/03/how-to-setup-your-first-spark-project-in-intellij-ide/
// http://scalatutorials.com/beginner/2013/07/18/getting-started-with-sbt/