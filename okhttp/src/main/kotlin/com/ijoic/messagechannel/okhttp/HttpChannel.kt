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

import com.ijoic.messagechannel.RequestChannel
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Proxy

/**
 * Http channel
 *
 * @author verstsiu created at 2019-10-15 18:10
 */
class HttpChannel(private val options: Options) : RequestChannel() {

  constructor(url: String): this(Options(url))

  private val request by lazy { Request.Builder().url(options.url).build() }
  private val client = OkHttpClient.Builder()
    .apply {
      val host = options.proxyHost
      val port = options.proxyPort

      if (!host.isNullOrBlank() && port != null) {
        proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(host, port)))
      }
    }
    .build()

  private val decodeResponse = options.decodeResponse ?: { it.body?.string() }

  override fun doRequestMessage() {
    try {
      val response = client.newCall(request).execute()
      val receiveTime = getCurrentTime()
      val result = decodeResponse(response)

      if (result != null) {
        onMessage?.invoke(receiveTime, result)
      } else {
        throw IOException("response empty")
      }
      logInfo("request complete")

    } catch (e: Exception) {
      onError?.invoke(e)
      logError("request failed", e)
    }
  }

  override fun toString(): String {
    return options.url
  }

  /**
   * Options
   */
  data class Options(
    val url: String,
    val proxyHost: String? = null,
    val proxyPort: Int? = null,
    val decodeResponse: ((Response) -> Any?)? = null
  )

}