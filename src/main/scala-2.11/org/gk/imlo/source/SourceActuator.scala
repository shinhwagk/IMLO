package org.gk.imlo.source

import java.sql.DriverManager
import java.util.Properties
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map, Set}

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

    s"select $columns from $table where $primaryKey >=? and $primaryKey <= ?"
  }

  def getConnect = {
    val ip = sourceInfo("ip")
    val port = sourceInfo("port")
    val serviceName = sourceInfo("servicename")
    val username = sourceInfo("username")
    val password = sourceInfo("password")
    val url = s"jdbc:oracle:thin:@$ip:$port/$serviceName"
    val props = new Properties()
    props.put("oracle.jdbc.ReadTimeout", "6000")
    props.put("user", username)
    props.put("password", password)
    DriverManager.getConnection(url, props)
  }

  def getRows(tid: Long) = {
    val conn = getConnect
    val stmt = conn.prepareStatement(sql)
    stmt.setLong(1, tid)
    stmt.setLong(2, tid + 5000)
    val rs = stmt.executeQuery()
    val colNum = rs.getMetaData.getColumnCount
    val rows = ArrayBuffer[Array[(String,Any)]]()
    while (rs.next()) {
      val row = new Array[(String,Any)](colNum)
      for (i <- 1 to colNum) {
        val colType = rs.getMetaData.getColumnTypeName(i)
        val colName = rs.getMetaData.getColumnName(i)
        colType match {
          case "NUMBER" =>
            row(i) = ("Long", rs.getLong(i))
          case "VARCHAR2" =>
            row(i) = ("String", rs.getString(i))
          case "DATE" =>
            row(i) = ("Timestamp", rs.getTimestamp(i))
        }
      }
      rows += row
    }
    rs.close()
    stmt.close()
    conn.close()
    (tid, rows)
  }
}