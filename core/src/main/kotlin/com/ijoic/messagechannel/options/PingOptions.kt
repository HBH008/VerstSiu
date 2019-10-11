package com.ijoic.messagechannel.options

import java.time.Duration

/**
 * Ping options
 *
 * @author verstsiu created at 2019-10-11 15:25
 */
data class PingOptions(
  val enabled: Boolean = true,
  val pingMessage: Any? = null,
  val pongMessage: Any? = null,
  val genPingMessage: (() -> Any)? = null,
  val isPongMessage: ((Any) -> Boolean)? = null,
  val pingInterval: Duration? = null,
  val pingAfterNoMessageReceived: Duration? = null,
  val messageReceivedTimeout: Duration? = null,
  val pongReceivedTimeout: Duration? = null
)