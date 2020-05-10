package com.example.spark.transformations

/* ************************************** Map Higher order Function **********************************************
 *  The map method is a higher-order method that takes a function as input and applies it to each element in the source RDD
 *  ************************************************************************************************************ */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object MapPartitionDemo {
  
   def main(args : Array[String]){
     
      val sparkSession = SparkSession.builder().appName("MapPartitions Higher order Function").master("local[*]").getOrCreate();
      
      val sparkContext = sparkSession.sparkContext;
      
      sparkContext.setLogLevel("ERROR");
      
      println("############################ MapPartitions Function Example ####################################");
      
      val fileRdd = sparkContext.textFile("src/main/resources/dataset/employee.csv");
      
       val header = fileRdd.first();
      
      val fileContentRdd = fileRdd.filter(line => line != header).mapPartitions ( x => x.filter( l => l.length>20));
         
      fileContentRdd.foreach(println);
     
   }
}