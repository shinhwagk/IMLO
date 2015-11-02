package org.gk.imlo.control

import java.sql.DriverManager
import java.util.Properties

import slick.driver.MySQLDriver.api._

/**
 * Created by gk on 2015/10/13.
 */
object Checkpoint {
  Class.forName("oracle.jdbc.driver.OracleDriver");
  private val url = s"jdbc:oracle:thin:@192.168.20.216:1521/biee";
  val props = new Properties();
  props.put("oracle.jdbc.ReadTimeout", "6000");
  props.put("user", "bigdata");
  props.put("password", "bigdata");
  val conn = DriverManager.getConnection(url, props)
  val stmt = conn.prepareStatement("update checkpoint set success = 1 where STARTID = ?")
  def markSuccess(id: Int) = {
    stmt.setInt(1, id)
    stmt.execute()
    stmt.clearParameters()
  }
}


object ImportTable {

  class Checkpoint(tag: Tag) extends Table[(Int, Int)](tag, "checkpoint") {
    def START_ID = column[Int]("START_ID", O.PrimaryKey)

    def SUCCESS = column[Int]("SUCCESS")

    def * = (START_ID, SUCCESS)
  }

  val checkpointTab = TableQuery[Checkpoint]
}