package test

/**
  * Created by gk on 2015/11/24.
  */
object Logger {
  def info(msg: String): Unit = synchronized {
    println("Info: " + msg)
  }
}