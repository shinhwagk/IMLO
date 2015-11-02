package org.gk.imlo.source

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.gk.imlo.Message.RowsInfo

import scala.collection.mutable.{ArrayBuffer, Map}

/**
 * Created by gk on 2015/10/22.
 */
object SourceActuator {

  val sourceInfo = {
    import scala.io.Source
    val tmpMap = Map[String,String]()
    for (i <- Source.fromFile("abc").getLines()) {
      val kv = i.split("=")
      val key = kv(0)
      if (kv.length == 2) {
        val value = kv(1)
        if (value != "null") {
          tmpMap += (key -> value)
        }
      } else {
        throw new Exception(s"key:$key 没有值,如果没有请填写null")
      }
    }
    tmpMap
  }

  lazy val sql = {
    val table = sourceInfo("table")
    val columns = sourceInfo("columns")
    val primaryKey = sourceInfo("primarykey")

    s"SELECT $columns FROM $table WHERE $primaryKey >=? AND $primaryKey < ?"
  }

  val ip = sourceInfo("ip")
  val port = sourceInfo("port")
  val serviceName = sourceInfo("servicename")
  val username = sourceInfo("username")
  val password = sourceInfo("password")
  val url = s"jdbc:oracle:thin:@$ip:$port/$serviceName"

  val ds = {
    //    val config = new HikariConfig();
    val ds = new HikariDataSource();
    ds.setJdbcUrl(url);
    ds.setUsername(username);
    ds.setPassword(password);
    ds.setMaximumPoolSize(15)
//    ds.setMaxLifetime(60000l)
    ds.setIdleTimeout(10000l)
    ds.addDataSourceProperty("cachePrepStmts", "true");
    ds.addDataSourceProperty("prepStmtCacheSize", "250");
    ds.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    ds
    //    config.addDataSourceProperty("cachePrepStmts", "true");
    //    config.addDataSourceProperty("prepStmtCacheSize", "250");
    //    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    //    new HikariDataSource(config);
  }


  def getRows(tid: Long) = {
    val conn = ds.getConnection
    val stmt = conn.prepareStatement(sql)
    stmt.setLong(1, tid)
    stmt.setLong(2, tid + 5000)
    val rs = stmt.executeQuery()
    val colNum = rs.getMetaData.getColumnCount
    val rows = ArrayBuffer[Array[(String,String,Any)]]()
    try {
      while (rs.next()) {
        val row = new Array[(String, String, Any)](colNum)
        //数组从0开始，但是jdbc数据从1开始.
        for (i <- 0 to colNum - 1) {
          val colType = rs.getMetaData.getColumnTypeName(i + 1)
          val colName = rs.getMetaData.getColumnName(i + 1)
          colType match {
            case "NUMBER" =>
              row(i) = ("Long", colName, rs.getLong(i + 1))
            case "VARCHAR2" =>
              row(i) = ("String", colName, rs.getString(i + 1))
            case "DATE" =>
              row(i) = ("Timestamp", colName, rs.getTimestamp(i + 1))
          }
        }
        rows += row
      }
      println(tid, rows.size)
      RowsInfo(tid, rows)
    } catch {
      case ex: Exception => throw ex
    }
    finally {
      rs.close()
      stmt.close()
      conn.close()
    }
  }

  def main(args: Array[String]) {
    println(getRows(300000000).rows(0).map(_._1).toList)
  }
}