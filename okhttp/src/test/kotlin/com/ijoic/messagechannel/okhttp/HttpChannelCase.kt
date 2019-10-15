package com.ijoic.messagechannel.okhttp

import com.ijoic.messagechannel.RequestChannel

fun main() {
  val channel: RequestChannel = HttpChannel("https://www.google.com")
  channel.onMessage = { _, message -> println(message) }
  channel.onError = { it.printStackTrace() }
  channel.prepare()
  Thread.sleep(2000)
  channel.close()
}