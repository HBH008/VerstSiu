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
  val mapPongMessage: ((Any) -> Any?)? = null,
  val isPongMessage: ((Any) -> Boolean)? = null,
  val pingInterval: Duration? = null,
  val pingAfterNoMessageReceived: Duration? = null,
  val messageReceivedTimeout: Duration? = null,
  val pongReceivedTimeout: Duration? = null
)