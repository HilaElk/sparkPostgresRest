package com.spark

import java.util.Properties

object JDBCConnector {


  val connectionCfg: Properties = {
    val props = new Properties()
    props.put("url", "jdbc:postgresql://localhost")
    props.put("port", "5432")
    props.put("dbName", "wiki")
    props.put("user", "postgres")
    props.put("password", "postgres")
    props
  }

  val purl = "jdbc:postgresql://myrdsinstance.cyqhcbzbuvov.us-east-2.rds.amazonaws.com/myRDSInstance?user=postgres&password=postgres"
  val url = "jdbc:postgresql://localhost/wiki?user=postgres&password=postgres"

  //val url =connectionCfg.getProperty("datasource.url")+":"+connectionCfg.getProperty("datasource.port")+"/"+connectionCfg.getProperty("datasource.dbName")+"?user="+connectionCfg.getProperty("datasource.user") +  "&password="+connectionCfg.getProperty("datasource.password");
}