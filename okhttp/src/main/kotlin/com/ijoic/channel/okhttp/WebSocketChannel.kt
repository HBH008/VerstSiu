/*
 *
 *  Copyright(c) 2020 VerstSiu
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.ijoic.channel.okhttp

import com.ijoic.channel.subscribe.ChannelWriter
import com.ijoic.channel.subscribe.ConnectionHandler
import com.ijoic.channel.subscribe.SubscribeChannel
import com.ijoic.channel.base.options.PingOptions
import com.ijoic.channel.base.options.RetryOptions
import okhttp3.*
import okio.ByteString
import java.lang.ref.WeakReference
import java.net.InetSocketAddress
import java.net.Proxy

/**
 * WebSocket channel
 *
 * @author verstsiu created at 2020-02-28 10:10
 */
class WebSocketChannel(
  url: String,
  prepareIntervalMs: Long = 1000L
) : SubscribeChannel(url, prepareIntervalMs, WebSocketHandler(Options(url))) {

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

      try {
        socket.close(1000, "client close connection")
      } catch (e: Exception) {
        e.printStackTrace()
      }
    }
  }

  private class WebSocketHandler(options: Options) : ConnectionHandler {

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

    private val decodeBytes: ((ByteArray) -> String?)? = options.decodeBytes

    override fun prepareConnection(listener: ConnectionHandler.StateListener) {
      client.newWebSocket(request, object : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
          listener.onConnectionComplete(WebSocketWriter(webSocket))
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
          listener.onConnectionFailure(t)
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
          webSocket.close(code, reason)
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
          listener.onConnectionClosed()
        }

        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
          val receiveTime = System.currentTimeMillis()

          if (decodeBytes != null) {
            val text = decodeBytes.invoke(bytes.toByteArray()) ?: return
            listener.onMessageReceived(receiveTime, text)
          } else {
            listener.onMessageReceived(receiveTime, bytes)
          }
        }

        override fun onMessage(webSocket: WebSocket, text: String) {
          val receiveTime = System.currentTimeMillis()
          listener.onMessageReceived(receiveTime, text)
        }
      })
    }
  }

  /**
   * Options
   */
  data class Options(
    val url: String,
    val prepareIntervalMs: Long = 1000L,
    val proxyHost: String? = null,
    val proxyPort: Int? = null,
    val pingOptions: PingOptions? = null,
    val retryOptions: RetryOptions? = null,
    val decodeBytes: ((ByteArray) -> String?)? = null
  )
}