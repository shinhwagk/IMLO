import akka.actor.ActorSystem
import akka.stream.{OverflowStrategy, ActorMaterializer}
import akka.stream.scaladsl._
import com.datastax.driver.core.{ResultSetFuture, ResultSet}
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, CanAwait, TimeoutException, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try

/**
 * Created by gk on 2015/10/20.
 */
object dfdfd extends App {
    implicit val system = ActorSystem("ClusterSystem")
    implicit val materializer = ActorMaterializer()
    Source(1 to 10).via(Flow[Int].map(_*10)).to(Sink.foreach(println))
  ////  val in = Source(1 to 3)
  ////  val f1 = Flow[Int].mapAsyncUnordered(3) { i => Future[Int]{println(s"B: $i");Thread.sleep(5000); i} }
  ////  val f2 = Flow[Int].map { i => println(s"B: $i");Thread.sleep(5000); i }
  ////  in.via(f2).runWith(Sink.foreach(println))
  ////  //  Source(1 to 10).via(Flow[Int].map{p=>println("flow");Thread.sleep(10000);p + 10}.buffer(10,OverflowStrategy.backpressure)).to(Sink.foreach[Int](println)).run()
  ////  val topHeadSink = Sink.head[Int]
  ////  val bottomHeadSink = Sink.head[Int]
  ////  val sharedDoubler = Flow[Int].map(_ * 2)
  //
  //  val pairUpWithToString = Flow() { implicit b =>
  //    import FlowGraph.Implicits._
  //
  //    val broadcast = b.add(Broadcast[Int](1))
  //    val zip = b.add(Merge[Int](1))
  //
  //    broadcast.out(0).map(_*10) ~> zip
  //
  //    (broadcast.in, zip.out)
  //  }
  //
  //Source(1 to 3).buffer(10,OverflowStrategy.backpressure).via(pairUpWithToString).runWith(Sink.foreach(println))


  lazy val ssss = for (i <- 1 to 3) yield i * 5
  val bb = ssss
  val b = bb.takeRight(2)
  println(bb)
  println(b)
  //  println(ssss.head)
}

class abc(resultSetFuture: ResultSetFuture) extends Future[ResultSet]{
  override def onComplete[U](f: (Try[ResultSet]) => U)(implicit executor: ExecutionContext): Unit = ???

  override def isCompleted: Boolean = resultSetFuture.isDone

  override def value: Option[Try[ResultSet]] = ???

  @throws[Exception](classOf[Exception])
  override def result(atMost: Duration)(implicit permit: CanAwait): ResultSet = {
    resultSetFuture.getUninterruptibly
  }

  @throws[InterruptedException](classOf[InterruptedException])
  @throws[TimeoutException](classOf[TimeoutException])
  override def ready(atMost: Duration)(implicit permit: CanAwait): abc.this.type = ???
}
