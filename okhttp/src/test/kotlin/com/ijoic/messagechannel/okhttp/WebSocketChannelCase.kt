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

fun main() {
  val channel: MessageChannel = WebSocketChannel("wss://echo.websocket.org")
  channel.onOpen = { println("connection opened") }
  channel.onMessage = { _, message -> println(message) }
  channel.onError = { it.printStackTrace() }
  channel.onClosed = { println("connection closed") }
  channel.send("hello world!")
  Thread.sleep(2000)
  channel.close()
}