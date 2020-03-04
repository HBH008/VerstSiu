package com.ijoic.channel.okhttp

fun main() {
  val channel = WebSocketChannel("wss://echo.websocket.org")
  channel.setup()
  Thread.sleep(10000L)
  channel.close()
}