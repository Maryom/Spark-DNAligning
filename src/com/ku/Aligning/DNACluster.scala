package com.ku.Aligning

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.mllib.rdd.RDDFunctions._
import scala.io.Source

object DNACluster {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    /** Load up the DNA reference */
    def load(filename: String) : String = {

      val bufferedSource = Source.fromFile(s"$filename")
    
      val ref = bufferedSource.getLines.mkString

      bufferedSource.close
    
      return ref
    }

    // Start the timer
    val t1 = System.nanoTime

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the cluster
    val conf = new SparkConf()
    conf.setAppName("DNA")
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.shuffle.service.enabled", "true")
    val sc = new SparkContext(conf)
    
    // read the passing arguments
    val partitions    = args(0)
    val readLength    = args(1)
    val refFile       = args(2)
    val inputFile     = args(3)
    val outputFile    = args(4)
    val readsLength   = readLength.toInt
    val partitionsNum = partitions.toInt
    
    val DNAref = sc.broadcast(load(refFile))

    val DNALength = DNAref.value.length

    val indexing = sc.parallelize(List.range(0, DNALength), partitionsNum).map(index => (DNAref.value.slice(index, index+readsLength), index)).persist

    val DNAReads = sc.textFile(s"s3n://$inputFile").sliding(2,2).map(IDread => (IDread(1), IDread(0).substring(1))).persist

    val indexingPartitioner = new RangePartitioner(partitionsNum, indexing)
    val readsPartitioner    = new RangePartitioner(partitionsNum, DNAReads)
    
    val sortedIndexes = indexing.partitionBy(indexingPartitioner).sortByKey().persist
    val sortedReads   = DNAReads.partitionBy(readsPartitioner).sortByKey().persist

    // removing rdd data (cleaning)
    indexing.unpersist()
    DNAReads.unpersist()
    
    List("a","c","g","t").foreach { base =>
      
      val baseRDD   = sortedIndexes.filterByRange(base+"a.*", base+"tt.*").partitionBy(new HashPartitioner(partitionsNum)).persist
      val reads = sortedReads.filterByRange(base+"a.*", base+"tt.*").partitionBy(new HashPartitioner(partitionsNum)).persist
    
      val readsResults = reads.join(baseRDD).map(_._2).persist

      readsResults.saveAsTextFile(s"s3://$outputFile"+base)

      // removing rdd data (cleaning)
      baseRDD.unpersist()
      reads.unpersist()
      readsResults.unpersist() 
    }
  
    // removing rdd data (cleaning)
    sortedReads.unpersist()
    sortedIndexes.unpersist()
    
    // As it is a nano second we need to divide it by 1000000000. in 1e9d "d" stands for double
    val duration = (System.nanoTime - t1) / 1e9d

    println("Timer", duration)
  }
}