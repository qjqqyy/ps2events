package net.b0ss.ps2events

import net.b0ss.ps2events.Ps2ApiWsClient._
import sttp.client3._
import sttp.client3.httpclient.HttpClientFutureBackend
import sttp.ws.WebSocket

import java.util.{ Timer, TimerTask }
import scala.concurrent._
import scala.concurrent.duration.DurationInt
import scala.util.Random

class Ps2ApiWsClient(serviceId: String, consume: String => Unit)(implicit ec: ExecutionContext) {
  private val backend = HttpClientFutureBackend()
  private val timer = new Timer("ps2-ws-client-quit-timer")
  private var wsOption: Option[WebSocket[Future]] = None

  private def consumeAllMessages(ws: WebSocket[Future]): Future[Unit] =
    ws.receiveText()
      .map(consume)
      .flatMap(_ => consumeAllMessages(ws))

  private def handleWebsocket(ws: WebSocket[Future]): Future[Unit] = {
    wsOption = Some(ws)
    timer.schedule(
      new TimerTask {
        def run(): Unit = ws.close()
      },
      3 * 60 * 1000 + Random.nextInt(3 * 60 * 1000),
    )
    ws.sendText(SUBSCRIBE_ALL)
      .flatMap(_ => consumeAllMessages(ws))
  }

  def start(): Future[Response[Either[String, Unit]]] = basicRequest
    .get(uri"wss://push.planetside2.com/streaming?environment=ps2&service-id=s:$serviceId")
    .response(asWebSocket(handleWebsocket))
    .send(backend)

  def close(): Unit = {
    timer.cancel()
    try {
      Await.result(Future.sequence(wsOption.map(_.close()).toVector :+ backend.close()), 5.seconds)
    } catch {
      case _: java.util.concurrent.TimeoutException =>
    }
  }

}

object Ps2ApiWsClient {
  final val SUBSCRIBE_ALL =
    """{
      |  "service": "event",
      |  "action": "subscribe",
      |  "characters": ["all"],
      |  "worlds": ["all"],
      |  "eventNames": ["all"]
      |}""".stripMargin
}
