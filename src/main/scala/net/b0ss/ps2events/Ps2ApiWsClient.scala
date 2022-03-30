package net.b0ss.ps2events

import net.b0ss.ps2events.Ps2ApiWsClient._
import sttp.client3._
import sttp.client3.httpclient.HttpClientFutureBackend
import sttp.ws.WebSocket

import scala.concurrent.{ ExecutionContext, Future }

class Ps2ApiWsClient(serviceId: String, consume: String => Unit)(implicit ec: ExecutionContext) {

  private val backend = HttpClientFutureBackend()

  private def consumeAllMessages(ws: WebSocket[Future]): Future[Unit] =
    ws.receiveText()
      .map(consume)
      .flatMap(_ => consumeAllMessages(ws))

  private def handleWebsocket(ws: WebSocket[Future]): Future[Unit] = {
    for {
      _ <- ws.sendText(SUBSCRIBE_ALL)
      _ <- consumeAllMessages(ws)
    } yield ()
  }

  def start(): Future[Response[Either[String, Unit]]] = basicRequest
    .get(uri"wss://push.planetside2.com/streaming?environment=ps2&service-id=s:$serviceId")
    .response(asWebSocket(handleWebsocket))
    .send(backend)

  def close(): Future[Unit] = backend.close()

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
