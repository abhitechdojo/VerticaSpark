package com.abhi

/**
  * Created by abhishek.srivastava on 1/12/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
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
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[VerticaRecord])
  job.setOutputFormatClass(classOf[VerticaOutputFormat])

  VerticaInputFormat.setInput(job, "select * from Foo where key = ?", "1", "2", "3", "4")
  VerticaOutputFormat.setOutput(job, "Bar", true, "name varchar", "total int")
  val rddVR : RDD[VerticaRecord] = sc.newAPIHadoopRDD(job.getConfiguration, classOf[VerticaInputFormat], classOf[LongWritable], classOf[VerticaRecord]).map(_._2)
  // convert rdd of vertica records to rdd of tuples. first is name second is val
  val rddTup = rddVR.map(x => (x.get(1).toString(), x.get(2).toString().toInt))
  // group by
  val rddGroup = rddTup.reduceByKey(_ + _)
  val rddVROutput = rddGroup.map({
    case(x, y) => (new Text("Bar"), getVerticaRecord(x, y, job.getConfiguration))
  })

  //rddVROutput.saveAsNewAPIHadoopFile("Bar", classOf[Text], classOf[VerticaRecord], classOf[VerticaOutputFormat], job.getConfiguration)
  rddVROutput.saveAsNewAPIHadoopDataset(job.getConfiguration)

  def getVerticaRecord(name : String, value : Int , conf: Configuration) : VerticaRecord = {
    println("++++++++++++++ CAME INSIDE +++++++++++++++++ ")
    var retVal = new VerticaRecord(conf)
    //println(s"going to build Vertica Record with ${name} and ${value}")
    retVal.set(0, new Text(name))
    retVal.set(1, new IntWritable(value))
    retVal
  }
}