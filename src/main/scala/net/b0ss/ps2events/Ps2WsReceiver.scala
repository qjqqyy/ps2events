package net.b0ss.ps2events

import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.streaming.receiver.Receiver

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class Ps2WsReceiver(serviceId: String) extends Receiver[String](MEMORY_AND_DISK) {

  @transient implicit private lazy val ec: ExecutionContext =
    ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  @transient private lazy val wsClient = new Ps2ApiWsClient(serviceId, store)

  override def onStart(): Unit = wsClient
    .start()
    .onComplete {
      case Success(value)     => restart(s"websocket connection closed, $value")
      case Failure(exception) => restart("websocket connection closed", exception)
    }

  override def onStop(): Unit = wsClient.close()
}
