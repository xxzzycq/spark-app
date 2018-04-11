package com.xxzzycq.util

import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * Created by yangchangqi on 2017/5/18.
  */
object SparkUtil {
  //保存parquet的文件到HDFS
  def saveDfToParquet(df : DataFrame, path : String): Unit = {
    df.write.mode(SaveMode.Overwrite).parquet(path)
  }

  //保存ORC文件到HDFS
  def saveDfToORC(df : DataFrame, path : String): Unit = {
    df.write.mode(SaveMode.Overwrite).orc(path)
  }

  //保存文本文件到HDFS
  def saveDfToText(df : DataFrame, path : String): Unit = {
    df.write.mode(SaveMode.Overwrite).text(path)
  }
}
