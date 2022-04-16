package net.b0ss.ps2events

import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.streaming.receiver.Receiver

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContext
import scala.util.{ Failure, Success }

class Ps2WsReceiver(serviceId: String) extends Receiver[String](MEMORY_AND_DISK) with Logging {

  @transient private lazy val executor = Executors.newSingleThreadExecutor()
  @transient implicit private lazy val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

  @transient private lazy val wsClient = new Ps2ApiWsClient(serviceId, store)

  @transient private lazy val shouldShutdown = new AtomicBoolean()

  override def onStart(): Unit = {
    shouldShutdown.set(true)
    wsClient
      .start()
      .andThen(_ => shouldShutdown.set(false))
      .onComplete {
        case Success(value) =>
          restart(s"websocket connection closed, got $value, shouldShutdown = ${shouldShutdown.get()}")
        case Failure(exception) =>
          restart(s"websocket connection closed, shouldShutdown = ${shouldShutdown.get()}", exception)
      }
  }

  override def onStop(): Unit = if (shouldShutdown.get()) shutdownNow()

  private def shutdownNow(): Unit = {
    logWarning("shutting down forcefully")
    wsClient.close()
    executor.shutdownNow()
  }

}
