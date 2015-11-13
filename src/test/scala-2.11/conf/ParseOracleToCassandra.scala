package conf

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * Created by gk on 2015/11/13.
  */
class ParseOracleToCassandra(confName: String) {
  private val a = Source.fromFile(confName)
  private val confInfo = a.getLines().toArray
  private val oracleConnection = new ArrayBuffer[String]()
  private var oracleSql: String = _
  private val CassandraConnection = new ArrayBuffer[String]()
  private var CassandraCql: String = _

  lazy val parOracleConnectionString = oracleConnection(0)
  lazy val parOracleConnectionUsername = oracleConnection(1)
  lazy val parOracleConnectionPassword = oracleConnection(2)
  lazy val parOracleSql = oracleSql

  lazy val parCassandraIp = CassandraConnection(0)
  lazy val parCassandraKeyspace = CassandraConnection(1)
  lazy val parCassandraCql = CassandraCql

  for (i <- 0 until confInfo.length) {
    confInfo(i) match {
      case "[Oracle Connection]" =>
        oracleConnection += confInfo(i + 1)
        oracleConnection += confInfo(i + 2)
        oracleConnection += confInfo(i + 3)
      case "[Oracle Sql]" =>
        oracleSql = confInfo(i + 1)
      case "[Cassandra Connection]" =>
        CassandraConnection += confInfo(i + 1)
        CassandraConnection += confInfo(i + 2)
      case "[Cassandra Cql]" =>
        CassandraCql = confInfo(i + 1)
      case _ =>

    }
  }
}