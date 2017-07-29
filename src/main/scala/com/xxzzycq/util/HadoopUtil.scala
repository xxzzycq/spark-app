package com.xxzzycq.util

import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration

/**
  * Created by yangchangqi on 2017/5/18.
  */
object HadoopUtil {

  //获取fs对象
  def getFileSystem(): FileSystem = {
    val conf : Configuration  = new Configuration()
    FileSystem.get(conf)
  }

  //根据输入目录计算目录大小，并以128M大小计算partition
  def getRePartitions(sourcePath : String, fs : FileSystem) : Int ={
    val cos:ContentSummary = fs.getContentSummary(new Path(sourcePath))
    val sizeM = cos.getLength / 1024 / 1024
    val partNum = sizeM / 128 match {
      case 0 => 1
      case _ => (sizeM / 128).toInt
    }
    partNum
  }

  //创建临时目录
  def makeTmpDir(tmpPath : String, fs : FileSystem) : Unit = {
    val path = new Path(tmpPath)
    if (!fs.exists(path)) {
      fs.mkdirs(path)
    }
  }

  //删除临时目录
  def deleteTmpDir(tmpPath : String, fs : FileSystem) : Unit = {
    val path = new Path(tmpPath)
    if(fs.exists(path)) {
      // if path is a directory and set to true, the directory is deleted else throws an exception
      fs.delete(path,true)
    }
  }

  //删除目标目录下的文件
  def deleteFiles(tmpPath : String, fs : FileSystem) : Unit = {
    val files = FileUtil.stat2Paths(fs.listStatus(new Path(tmpPath)))
    for (f <- files) {
      // 迭代删除文件或目录
      if (fs.isFile(f)) {
        fs.delete(f, true)
      }
    }
  }

}
