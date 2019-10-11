package com.ijoic.messagechannel.okhttp

import com.ijoic.messagechannel.Channel
import com.ijoic.messagechannel.ChannelWriter
import com.ijoic.messagechannel.options.RetryOptions
import okhttp3.*
import okio.ByteString
import java.lang.IllegalArgumentException
import java.lang.ref.WeakReference

/**
 * WebSocket channel
 *
 * @author verstsiu created at 2019-10-08 11:02
 */
class WebSocketChannel(
  private val url: String,
  options: RetryOptions? = null) : Channel(options) {

  override fun onPrepareConnection() {
    val request = Request.Builder().url(url).build()
    val client = OkHttpClient()

    client.newWebSocket(request, object : WebSocketListener() {
      override fun onOpen(webSocket: WebSocket, response: Response) {
        notifyConnectionComplete(WebSocketWriter(webSocket))
      }

      override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        notifyConnectionFailure(t)
      }

      override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        notifyConnectionClosed()
      }

      override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        notifyMessageReceived(bytes)
      }

      override fun onMessage(webSocket: WebSocket, text: String) {
        notifyMessageReceived(text)
      }
    })
  }

  /**
   * WebSocket writer
   */
  private class WebSocketWriter(socket: WebSocket) : ChannelWriter {
    private var refSocket: WeakReference<WebSocket>? = WeakReference(socket)

    override fun write(message: Any) {
      val socket = refSocket?.get() ?: return

      when (message) {
        is String -> socket.send(message)
        is ByteString -> socket.send(message)
        else -> throw IllegalArgumentException("invalid message type: ${message.javaClass.canonicalName}")
      }
    }

    override fun close() {
      val socket = refSocket?.get() ?: return
      refSocket = null
      socket.close(1000, "client close connection")
    }
  }

}