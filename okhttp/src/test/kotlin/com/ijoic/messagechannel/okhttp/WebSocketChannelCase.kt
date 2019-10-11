package com.ijoic.messagechannel.okhttp

import com.ijoic.messagechannel.Channel

fun main() {
  val channel: Channel = WebSocketChannel("wss://echo.websocket.org")
  channel.onOpen = { println("connection opened") }
  channel.onMessage = { println(it) }
  channel.onError = { it.printStackTrace() }
  channel.onClosed = { println("connection closed") }
  channel.send("hello world!")
  Thread.sleep(2000)
  channel.close()
}