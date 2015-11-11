package test.meta.source

import java.sql.DriverManager
import java.util.Properties

import com.zaxxer.hikari.HikariDataSource
import org.gk.imlo.Message.RowsInfo
import test.meta.MetaDB

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Created by gk on 2015/11/9.
  */
class Oracle(sourceInfo: MetaDB.InfoJdbc) {
  private val info = sourceInfo
  private val ip = info.ip
  private val port = info.port
  private val serviceName = info.service
  private val username = info.username
  private val password = info.password
  private val table = info.tablename
  private val columns = info.columns
  private val primaryKey = info.primarykey
  private val whereString = info.whereString
  private val step = info.step
  private val url = s"jdbc:oracle:thin:@$ip:$port/$serviceName"

  private val sql = {
    s"SELECT $columns FROM $table WHERE $primaryKey >=? AND $primaryKey < ?"
  }

  def getRowSet(keyNum2: Int) = {
    val url = "jdbc:oracle:thin:@218.202.225.211:1521/sh11"
    val props = new Properties();
    props.put("oracle.jdbc.ReadTimeout", "6000");
    props.put("user", "andrstore");
    props.put("password", "andrstore");
    val conn = DriverManager.getConnection(url, props)
    println("开始处理", keyNum2)
    val keyNum = (keyNum2 * 5000).toLong
    //    val conn = ds.getConnection
    println(conn, keyNum2)
    val stmt = conn.prepareStatement(sql)
    stmt.setFetchSize(10000)
    stmt.setLong(1, keyNum)
    stmt.setLong(2, keyNum + step.toLong)
    val rs = stmt.executeQuery()
    val colNum = rs.getMetaData.getColumnCount
    val rows = ArrayBuffer[Array[(String, String, Any)]]()

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

    println(keyNum2, "执行完毕", rows.size)
    Future {
      conn.close()
    }
    RowsInfo(keyNum2.toLong, rows)
  }

  val ds = synchronized {
    val ds = new HikariDataSource()
    ds.setJdbcUrl(url)
    ds.setUsername(username)
    ds.setPassword(password)
    ds.setMaximumPoolSize(30)
    ds.setMinimumIdle(8)
    ds.setConnectionTimeout(10000l)
    //    ds.setMaxLifetime(60000l)
    ds.setIdleTimeout(5000)
    ds
  }
}