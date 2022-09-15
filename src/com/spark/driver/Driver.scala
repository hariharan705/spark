import org.apache.spark.sql.SparkSession


object Driver {
  def main(args:Array[String])={
//   simple word count program to count(rdd) the number of repeated countries   
     val sparkSession = SparkSession.builder.master("local[*]").appName("wordcount").getOrCreate()
     val sc=sparkSession.sparkContext
     val rdd=sc.textFile("/home/hariharan/sparkdatasource/BroadCastData.csv",2)
     val rdd1=rdd.map(r=>(r.split(",")(1))).map(r=>(r,1)).reduceByKey(_+_)
     rdd1.saveAsTextFile("/home/hariharan/sparkoutput/rddwordcount")
     
     
      
     
     
     
    
    
    
  }
}