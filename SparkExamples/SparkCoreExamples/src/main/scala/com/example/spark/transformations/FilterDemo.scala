package com.example.spark.transformations

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession; 

/*  *************************************** Filter High order Function  **********************************************************************
 *  The filter method is a higher-order method that takes a Boolean function as input  and applies it to each element in the source RDD to create a new RDD.
*  * **************************************************************************************************************************************/

object FilterDemo {
    
       def main(args: Array[String]){
     
      val sparkSession = SparkSession.builder().appName("Filter High order function").master("local[*]").getOrCreate();
      
      val sparkContext = sparkSession.sparkContext;
      
      sparkContext.setLogLevel("ERROR");
      
      println("############################ Filter Function Example ####################################");
      
      val fileRdd = sparkContext.textFile("src/main/resources/dataset/employee.csv");
      
      val header = fileRdd.first();
      
      val fileContentRdd = fileRdd.filter(line => line != header);
      
      fileContentRdd.foreach(println);
      
      val filteredContentRdd = fileContentRdd.map(x=> x.split(","))
                                                                                                        .filter( y => y(1).equalsIgnoreCase("Selva"))
                                                                                                                    .map(z => (z(0),z(1),z(2),z(3) ));
      
      println("\n");
      
      println("Filtered Rdd Content: \n" );
      
      filteredContentRdd.foreach(println);
   } 
}