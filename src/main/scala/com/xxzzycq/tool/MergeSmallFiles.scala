package com.xxzzycq.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by yangchangqi on 2017/5/18.
  */
object MergeSmallFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("merge small files").getOrCreate()
    val rdd = spark.sparkContext.textFile("/user/hive/warehouse/hivetableinfo").persist()
    val arr = rdd.collect()
    val fs = HadoopUtil.getFileSystem()
    for(line <- arr) {
      val values : Array[String] = line.split(" ")
      val path = values(0)
      val fileType = values(1)
      if (fileType.equals("text")) {
        val df = spark.read.text(path)
        val reDf = df.coalesce(HadoopUtil.getRePartitions(path, fs))
        val tmpPath  = path + "_mergetmp"
        HadoopUtil.makeTmpDir(tmpPath, fs)
        SparkUtil.saveDfToText(reDf, tmpPath)
        val tmpDf = spark.read.text(tmpPath).persist(StorageLevel.MEMORY_AND_DISK)
        SparkUtil.saveDfToText(tmpDf, path)
        HadoopUtil.deleteTmpDir(tmpPath, fs)
        tmpDf.unpersist()
      }
      if (fileType.equals("parquet")) {
        val df = spark.read.parquet(path)
        val reDf = df.coalesce(HadoopUtil.getRePartitions(path, fs))
        val tmpPath  = path + "_mergetmp"
        HadoopUtil.makeTmpDir(tmpPath, fs)
        SparkUtil.saveDfToParquet(reDf, tmpPath)
        val tmpDf = spark.read.parquet(tmpPath).persist(StorageLevel.MEMORY_AND_DISK)
        SparkUtil.saveDfToParquet(tmpDf, path)
        HadoopUtil.deleteTmpDir(tmpPath, fs)
        tmpDf.unpersist()
      }
      if (fileType.equals("orc")) {
        val df = spark.read.orc(path)
        val reDf = df.coalesce(HadoopUtil.getRePartitions(path, fs))
        val tmpPath  = path + "_mergetmp"
        HadoopUtil.makeTmpDir(tmpPath, fs)
        SparkUtil.saveDfToORC(reDf, tmpPath)
        val tmpDf = spark.read.orc(tmpPath).persist(StorageLevel.MEMORY_AND_DISK)
        SparkUtil.saveDfToORC(tmpDf, path)
        HadoopUtil.deleteTmpDir(tmpPath, fs)
        tmpDf.unpersist()
      }
    }

    fs.close()
    rdd.unpersist()
    spark.stop()
  }
}
