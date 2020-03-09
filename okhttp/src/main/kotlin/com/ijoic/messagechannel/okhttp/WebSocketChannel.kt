/*
 *
 *  Copyright(c) 2019 VerstSiu
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
package com.ijoic.messagechannel.okhttp

import com.ijoic.messagechannel.MessageChannel
import com.ijoic.messagechannel.options.PingOptions
import com.ijoic.messagechannel.options.RetryOptions
import okhttp3.*
import okio.ByteString
import java.lang.ref.WeakReference
import java.net.InetSocketAddress
import java.net.Proxy

/**
 * WebSocket channel
 *
 * @author verstsiu created at 2019-10-08 11:02
 */
class WebSocketChannel(options: Options) : MessageChannel(
  options.url,
  options.prepareMs,
  WebSocketHandler(options),
  options.pingOptions,
  options.retryOptions
) {

  constructor(url: String): this(Options(url))

  /**
   * WebSocket writer
   */
  private class WebSocketWriter(socket: WebSocket) : ChannelWriter() {
    private var refSocket: WeakReference<WebSocket>? = WeakReference(socket)

    override fun onWriteMessage(message: Any): Boolean {
      val socket = refSocket?.get() ?: return false

      when (message) {
        is String -> socket.send(message)
        is ByteString -> socket.send(message)
        else -> throw IllegalArgumentException("invalid message type: ${message.javaClass.canonicalName}")
      }
      return true
    }

    override fun onClose() {
      val socket = refSocket?.get() ?: return
      refSocket = null
      socket.close(1000, "client close connection")
    }
  }

  private class WebSocketHandler(options: Options) : PrepareHandler() {

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

    override fun onPrepareConnection(listener: StateListener) {
      client.newWebSocket(request, object : WebSocketListener() {
        override fun onOpen(webSocket: WebSocket, response: Response) {
          logOutput?.info("connection open")
          listener.onConnectionComplete(WebSocketWriter(webSocket))
        }

        override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
          logOutput?.error("connection failure: ${response?.code}", t)
          listener.onConnectionFailure(t)
        }

        override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
          logOutput?.info("connection closing: $code - $reason")
          webSocket.close(code, reason)
        }

        override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
          logOutput?.info("connection closed: $code - $reason")
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
    val prepareMs: Long = 1000L,
    val proxyHost: String? = null,
    val proxyPort: Int? = null,
    val pingOptions: PingOptions? = null,
    val retryOptions: RetryOptions? = null,
    val decodeBytes: ((ByteArray) -> String?)? = null
  )

}