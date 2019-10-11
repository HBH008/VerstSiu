package com.ijoic.messagechannel.okhttp

import com.ijoic.messagechannel.Channel
import com.ijoic.messagechannel.ChannelWriter
import com.ijoic.messagechannel.options.RetryOptions
import okhttp3.*
import okio.ByteString
import java.lang.IllegalArgumentException
import java.lang.ref.WeakReference
import java.net.InetSocketAddress
import java.net.Proxy

/**
 * WebSocket channel
 *
 * @author verstsiu created at 2019-10-08 11:02
 */
class WebSocketChannel(options: Options) : Channel(options.retryOptions) {

  constructor(url: String): this(Options(url))

  private val request = Request.Builder()
    .url(options.url)
    .build()

  private val client = OkHttpClient.Builder()
    .apply {
      val host = options.proxyHost
      val port = options.proxyPort

      if (!host.isNullOrBlank() && port != null) {
        proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(host, port)))
      }
    }
    .build()

  override fun onPrepareConnection() {
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

  /**
   * Options
   */
  data class Options(
    val url: String,
    val proxyHost: String? = null,
    val proxyPort: Int? = null,
    val retryOptions: RetryOptions? = null
  )
}