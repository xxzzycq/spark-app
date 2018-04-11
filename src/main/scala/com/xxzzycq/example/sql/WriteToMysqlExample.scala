package com.xxzzycq.example.sql

import com.xxzzycq.util.ConfigLoaderService

/**
  * Created by yangchangqi on 2018/3/8.
  */
object WriteToMysqlExample {
  def main(args: Array[String]): Unit = {
    print(ConfigLoaderService.getMysqlURL())
  }
}
