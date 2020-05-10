package com.example.spark.transformations

/* ************************************** Map Higher order Function **********************************************
 *  The map method is a higher-order method that takes a function as input and applies it to each element in the source RDD
 *  ************************************************************************************************************ */

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object MapDemo {
  
   def main(args : Array[String]){
     
      val sparkSession = SparkSession.builder().appName("Map Higher order Function").master("local[*]").getOrCreate();
      
      val sparkContext = sparkSession.sparkContext;
      
      sparkContext.setLogLevel("ERROR");
      
      println("############################ Map Function Example ####################################");
      
      val fileRdd = sparkContext.textFile("src/main/resources/dataset/employee.csv");
      
       val header = fileRdd.first();
      
      val fileContentRdd = fileRdd.filter(line => line != header).map( x => x.split(","));
      
      val fileData = fileContentRdd.map(  y=> (y(0),y(1),y(2),y(3)));
      
      fileData.foreach(println);
     
   }
}