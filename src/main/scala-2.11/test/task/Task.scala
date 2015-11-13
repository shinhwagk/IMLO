package test.task

import test.meta.MetaDB
import test.meta.source.Oracle
import test.target.Cassandra

/**
  * Created by gk on 2015/11/10.
  */
class Task(taskId: Int) {
  MetaDB.initTaskInfo(taskId)
  val source = new Oracle
  val cassandra  = new Cassandra

//  val taskInfo = MetaDB.getTaskInfo(taskId)

  MetaDB.updateChildExecing;

  def makeNewExecBuffer = synchronized {
    //    val maxChildTaskKeynum = MetaDB.getMaxChildTaskKeyNum
    if (MetaDB.getUnfinishedchildTask > 0) {
      val newKeyNum = MetaDB.getOnceUnfinishedchildTask
      newKeyNum
    } else {
      val startKeyNum = MetaDB.getMaxChildTaskKeyNum + 1
      println(startKeyNum,"dfdfsdfssssssssssssssss")
      MetaDB.makeNewKeyNum(startKeyNum)
      startKeyNum
    }
  }

  //  private val targetId = MetaDB.getInfoCassandra


  def getRowSet = {
    val keyNum = makeNewExecBuffer
    println("开始执行:", keyNum)
    source.getRowSet(keyNum)
  }

  def getRowSet(keyNum: Int) = {
    println("开始执行:", keyNum)
    source.getRowSet(keyNum)
  }

  def updateSuccessKeyNum(keyNum: Int) = {
    println(keyNum, "完成")
    MetaDB.updateChildCheckpoint(keyNum)
//    checkpoint
  }

//  def checkpoint: Unit = {
//    val minChildTaskSuccessKeyNum = MetaDB.getMinChildTaskKeyNum
////    val updateCount = MetaDB.updateCheckpoint(minChildTaskSuccessKeyNum)
//    println(minChildTaskSuccessKeyNum,updateCount,"更新梳理三")
//    if (updateCount > 0) {
//      MetaDB.deleteChildTask(minChildTaskSuccessKeyNum)
//      println(minChildTaskSuccessKeyNum, "wa昵称")
//      checkpoint
//    }
//  }
}
