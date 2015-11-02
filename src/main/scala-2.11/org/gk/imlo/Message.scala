package org.gk.imlo

import java.sql.DriverManager
import java.util.{Properties, Date}

import com.datastax.driver.core.ResultSetFuture
import org.gk.imlo.control.Checkpoint
import scala.collection.mutable.{ArrayBuffer, Map, Set}
import scala.collection.mutable.Map

/**
 * Created by gk on 2015/10/10.
 */
object Message {

  case class ImportRowToCassandraSuccess(a: ResultSetFuture, tid: Long)

  case object ConnectionReadTimeout

  case class SuccessAtid(aTid: Long)

  case class Row(value: Any, dataType: String)

  case class RowInfo(id: Long, rows: Set[Row])

  case class ExpdpInfo(ip: String, port: Int, servicename: String, username: String, passwd: String, sql: String, steps: Int, primarykey: String) {
    private val url = s"jdbc:oracle:thin:@$ip:$port/$servicename"
    private val props = new Properties()
    props.put("oracle.jdbc.ReadTimeout", "6000")
    props.put("user", "andrstore")
    props.put("password", "andrstore")

    def getcolInfos = {
      val conn = DriverManager.getConnection(url, props)
      val rs = conn.createStatement().executeQuery(sql)
      val colNum = rs.getMetaData.getColumnCount
      for (i <- 1 to colNum) yield rs.getMetaData.getColumnName(i) -> rs.getMetaData.getColumnTypeName(i)
    }

    def getConn = {
      DriverManager.getConnection(url, props)
    }

    def getExecSql = {
      if (sql.toUpperCase.contains("Tid")) {
        "select * from (" + sql + s") where $primarykey >= ? and $primarykey <= ?"
      } else {
        "select * from (" + sql.replaceFirst("select", "select tid ") + s") where $primarykey >= ? and $primarykey <= ?"
      }
    }
  }

  case class RowsInfo(key: Long, rows: ArrayBuffer[Array[(String, String, Any)]])

}

