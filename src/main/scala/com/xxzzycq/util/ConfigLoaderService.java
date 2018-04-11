package com.xxzzycq.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
  * Created by yangchangqi on 2018/3/8.
  */
public class ConfigLoaderService {
  private static Properties properties = new Properties();

  static {
    try {
      loadProperties();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void loadProperties() throws IOException {
    String path = Thread.currentThread().getContextClassLoader().getResource("config.properties").getPath();
    properties.load(new FileInputStream(path));
  }

  public static String getMysqlURL() {
    return properties.getProperty("jdbc.url");
  }

  public static String getMysqlUser() {
    return properties.getProperty("jdbc.username");
  }

  public static String getMysqlPassword(){
    return properties.getProperty("jdbc.password");
  }
}
