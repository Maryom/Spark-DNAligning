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
    val RNAME         = args(5) // reference long name
    val readsLength   = readLength.toInt
    val partitionsNum = partitions.toInt

    val DNAref = sc.broadcast(load(refFile))

    val DNALength = DNAref.value.length

    val indexing = sc.parallelize(List.range(0, DNALength), partitionsNum).map(index => (DNAref.value.slice(index, index+readsLength), index+1)).persist

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

      val indexes_readsResults = reads.join(baseRDD).map(index => (index._2._1, index._2._1+"\t0\t"+RNAME+"\t"+index._2._2.toString+"\t60\t"+readsLength.toString+"M\t*\t0\t0\t"+index._1+"\t*")).persist

      // save mapped reads
      indexes_readsResults.values.saveAsTextFile(s"s3://$outputFile"+base)

      // subtractByKey to eliminate the mapped reads
      val unmapped = reads.map(read_idx => (read_idx._2, read_idx._1)).subtractByKey(indexes_readsResults).map(id_read => id_read._1+"\t4\t*\t0\t0\t*\t*\t0\t0\t"+id_read._2+"\t*").persist

      // save unmapped reads
      unmapped.saveAsTextFile(s"s3://$outputFile"+base+"unmapped")

      // removing rdd data (cleaning)
      baseRDD.unpersist()
      reads.unpersist()
      indexes_readsResults.unpersist()
    }

    // removing rdd data (cleaning)
    sortedReads.unpersist()
    sortedIndexes.unpersist()

    // As it is a nano second we need to divide it by 1000000000. in 1e9d "d" stands for double
    val duration = (System.nanoTime - t1) / 1e9d

    println("Timer", duration)
  }
}
