package com.example.spark.rdd

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object CreatingSparkContext {
  
  def main(args: Array[String]){
    
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkContext creation");
    val sc = new SparkContext(sparkConf);
    
    sc.setLogLevel("ERROR");
    
    println("################## RDD creation Using Spark context #######################");
    
    val array = Array(1,2,3,4,5,6,7,8,9,0);
    
    val arrayRdd = sc.parallelize(array, 2);
    
    println("Number of elements in RDD:"+ arrayRdd.count());
    
    arrayRdd.foreach(println);
    
    
  }
}