package com.example.spark.transformations

/* **************************************Flat  Map Higher order Function ******************************************************
 *  The flatMap method is a higher-order method that takes an input function, which returns a sequence for each input element passed to it. 
 *  The flatMap method returns a new RDD formed by flattening this collection of sequence.
 *  ************************************************************************************************************************ */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object FlatMapDemo {
  
  def main(args : Array[String]){
     
      val sparkSession = SparkSession.builder().appName("FlatMap Higher order Function").master("local[*]").getOrCreate();
      
      val sparkContext = sparkSession.sparkContext;
      
      sparkContext.setLogLevel("ERROR");
      
      println("############################ FlatMap Function Example ####################################");
      
      val fileRdd = sparkContext.textFile("src/main/resources/dataset/employee.csv");
      
       val header = fileRdd.first();
      
      val fileContentRdd = fileRdd.filter(line => line != header).flatMap( x=> x.split(","));
            
      fileContentRdd.foreach(println);
     
   }
}