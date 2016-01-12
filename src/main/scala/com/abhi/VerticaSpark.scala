package com.abhi

/**
  * Created by abhishek.srivastava on 1/12/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.LongWritable
import com.vertica.hadoop._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.conf.Configuration

object VerticaSpark extends App {
  val scConf = new SparkConf
  val sc = new SparkContext(scConf)
  val conf = new Configuration()
  conf.set("mapred.vertica.database", "ddcanalytics")
  conf.set("mapred.vertica.port", "5433")
  conf.set("mapred.vertica.username", "vertica")
  conf.set("mapred.vertica.hostnames", "10.12.16.153,10.12.16.154,10.12.17.252,10.37.0.65")
  conf.set("mapred.vertica.password", "vertica")

  val job = new Job(conf)
  job.setInputFormatClass(classOf[VerticaInputFormat])
  VerticaInputFormat.setInput(job, "select * from Foo where key = ?", "1", "2", "3", "4")
  val rdd : RDD[VerticaRecord] = sc.newAPIHadoopRDD(job.getConfiguration, classOf[VerticaInputFormat], classOf[LongWritable], classOf[VerticaRecord]).map(_._2)
  rdd.collect().foreach(x => println(s"${x.get(0)} ${x.get(1)}"))
}