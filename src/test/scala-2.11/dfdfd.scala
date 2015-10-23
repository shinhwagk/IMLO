import java.sql.DriverManager
import java.util.Properties
import com.datastax.driver.core.Cluster

import scala.collection.mutable.{ArrayBuffer, Map, Set}

/**
 * Created by gk on 2015/10/20.
 */
object dfdfd extends App {
  val url = s"jdbc:oracle:thin:@218.202.225.211:1521/sh11";
  val props = new Properties();
  val mmm = Set[Map[String, String]]()
  props.put("oracle.jdbc.ReadTimeout", "6000");
  props.put("user", "andrstore");
  props.put("password", "andrstore");


  def typeInfo(sql: String): Set[Map[String, String]] = {
    lazy val conn = DriverManager.getConnection(url, props)
    val row = conn.prepareStatement("SELECT to_char(created,'yyyy') year,to_char(created,'mm') month,to_char(created,'dd') day,to_char(created,'hh24') hour,to_char(created,'mi') minute,to_char(created,'ss') second from tbllog where rownum<1000").executeQuery()
    val colNum = row.getMetaData.getColumnCount
    for (i <- 1 to colNum) {
      val colType = row.getMetaData.getColumnTypeName(i)
      val colName = row.getMetaData.getColumnName(i)
      mmm += Map(colName -> colType)
    }
    mmm
  }


  /*
  *
  *       colType match {
        case "NUMBER" => mmm += Map(colName -> colType)
        case "VARCHAR2" => mmm += Map(colName -> colType)
        case "DATE" => mmm += Map(colName -> colType)
        case _ => throw new Exception("没有匹配的类型!!!")
      }
  * */
//  abc(sqlStatment("dfdf"), "adf")

  def abc(mmm: ArrayBuffer[(String, Any, String)], tab: String) = {
    val head = s"insert into $tab"

    var name = ""
    for (i <- mmm) {
      if (i != mmm.last)
        name = name + i._1 + ","
      else
        name = name + i._1

    }
    println(name)
  }


  //  val cluster = Cluster.builder().addContactPoint("192.168.12.40").build();
  //  val session = cluster.connect("andrstore")
  //
  //  import scala.collection.JavaConversions._
  //
  //  val cols = session.getCluster.getMetadata.getKeyspace("andrstore").getTable("tbllog").getColumns
  //
  //  getInsertCqlsyntax
  //
  //  def getInsertCqlsyntax = {
  //
  //    var name = ""
  //    for (i <- cols) {
  //      if (i != cols.last)
  //        name = name + i.getName + ","
  //      else
  //        name = name + i.getName
  //
  //    }
  //    val colList = "(" + name + ")"
  //
  //    var name2 = ""
  //    for (i <- 1 to cols.size()) {
  //
  //      if (i != cols.size())
  //        name2 = name2 + "?" + ","
  //      else
  //        name2 = name2 + "?"
  //    }
  //    val colList2 = "values(" + name2 + ")"
  //
  //
  //    println(head + colList + colList2)
  //  }

  //  def tableColoumDataType(): Map[String, (Int, String)] = {
  //
  //    val map = Map[String,(Int,String)]()
  //    var colNum = 1
  //      columnType match {
  //        case "NUMBER" =>
  //          map += (columnName ->(colNum, "Long"))
  //          colNum += 1
  //        case "VARCHAR2" =>
  //          map += (columnName ->(colNum, "String"))
  //          colNum += 1
  //        case "DATE" =>
  //          map += (columnName ->(colNum, "Timestamp"))
  //          colNum += 1
  //      }
  //
  //  }
  //
  //  def dafd(rs: java.sql.ResultSet) = {
  //    val mm = Map[Any,String]()
  //    val m = tableColoumDataType()
  //    rs.getMetaData.getColumnType()
  //    while (rs.next()) {
  //      m.foreach { r =>
  //        val colType = r._2._2
  //        val colId = r._2._1
  //        val colName = r._1
  //
  //        val colVal = colType match {
  //          case "Long" =>
  //            rs.getLong(colId)
  //          case "String" =>
  //            rs.getString(colId)
  //          case "DATE" =>
  //            rs.getTimestamp(colId)
  //        }
  //
  //        mm += (colVal -> colType)
  //      }
  //    }
  //  }


}
