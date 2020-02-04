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
package com.ijoic.messagechannel.util

import com.ijoic.messagechannel.options.PingOptions
import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Ping manager
 *
 * @author verstsiu created at 2019-10-11 15:18
 */
internal class PingManager(
  private val executor: ScheduledExecutorService,
  private val options: PingOptions,
  private val onSendPingMessage: (Any) -> Unit,
  private val onCloseConnection: () -> Unit) {

  private val pingIntervalMs = options.pingInterval?.toVerifiedMsOrNull()
  private val pingLazyMs = options.pingAfterNoMessageReceived?.toVerifiedMsOrNull()

  private val receiveTimeoutMs = options.messageReceivedTimeout?.toVerifiedMsOrNull()
  private val pongTimeoutMs = options.pongReceivedTimeout?.toVerifiedMsOrNull()

  private var isConnectionActive = false
  private var lastReceivedTime = 0L

  private var pingTask: Future<*>? = null
  private var receiveTimeoutTask: Future<*>? = null
  private var pongTimeoutTask: Future<*>? = null
  private var lastPing = 0L

  /**
   * Connection complete
   */
  fun onConnectionComplete() {
    if (!options.enabled) {
      return
    }
    isConnectionActive = true
    lastReceivedTime = getCurrTime()

    if (pingIntervalMs != null) {
      schedulePingInterval(pingIntervalMs)
    } else {
      schedulePingLazy()
    }
    scheduleCheckReceiveTimeout()
  }

  /**
   * Connection failure
   */
  fun onConnectionFailure() {
    if (!options.enabled) {
      return
    }
    resetReceivedStatus()
  }

  /**
   * Connection closed
   */
  fun onConnectionClosed() {
    if (!options.enabled) {
      return
    }
    resetReceivedStatus()
  }

  /**
   * Receive message
   */
  fun onReceivedMessage(isPongMessage: Boolean = false) {
    if (!options.enabled || !isConnectionActive) {
      return
    }

    if (isPongMessage) {
      lastPing = 0L
      pongTimeoutTask?.checkAndCancel()
      pongTimeoutTask = null
    } else {
      lastReceivedTime = getCurrTime()
    }
    if (pingIntervalMs == null) {
      schedulePingLazy()
    }
    scheduleCheckReceiveTimeout()
  }

  /**
   * Check pong [message]
   */
  fun checkPingMessage(message: Any): Boolean {
    if (!options.enabled) {
      return false
    }
    return options.pingMessage == message || options.isPingMessage?.invoke(message) == true
  }

  /**
   * Check pong [message]
   */
  fun checkPongMessage(message: Any): Boolean {
    if (!options.enabled) {
      return false
    }
    return options.pongMessage == message || options.isPongMessage?.invoke(message) == true
  }

  /**
   * Release manager
   */
  fun release() {
    if (!options.enabled) {
      return
    }
    resetReceivedStatus()
  }

  /* -- check ping :begin -- */

  private fun schedulePingInterval(periodMs: Long) {
    pingTask = executor.scheduleAtFixedRate(
      this::onPingInterval,
      0,
      periodMs,
      TimeUnit.MILLISECONDS
    )
  }

  private fun onPingInterval() {
    if (!isConnectionActive) {
      return
    }
    generateAndSendPingMessage()
  }

  private fun schedulePingLazy() {
    if (pingLazyMs != null) {
      pingTask?.checkAndCancel()
      pingTask = executor.schedule(
        this::onPingLazy,
        pingLazyMs,
        TimeUnit.MILLISECONDS
      )
    }
  }

  private fun onPingLazy() {
    if (!isConnectionActive) {
      return
    }
    val lazyMs = this.pingLazyMs ?: return
    val currTime = getCurrTime()

    if (currTime - lastReceivedTime > lazyMs) {
      generateAndSendPingMessage()
    }
  }

  private fun generateAndSendPingMessage() {
    val message = options.pingMessage ?: options.genPingMessage?.invoke()

    if (message != null) {
      onSendPingMessage(message)
      scheduleCheckPongTimeout()

      if (lastPing == 0L) {
        lastPing = getCurrTime()
      }
    }
  }

  /* -- check ping :end -- */

  /* -- check timeout :begin -- */

  private fun scheduleCheckReceiveTimeout() {
    if (receiveTimeoutMs != null) {
      receiveTimeoutTask?.checkAndCancel()
      receiveTimeoutTask = executor.schedule(
        this::onCheckReceiveTimeout,
        receiveTimeoutMs,
        TimeUnit.MILLISECONDS
      )
    }
  }

  private fun onCheckReceiveTimeout() {
    if (!isConnectionActive) {
      return
    }
    val currTime = getCurrTime()
    val checkMs = receiveTimeoutMs ?: return

    if (lastReceivedTime > 0 && currTime - lastReceivedTime >= checkMs) {
      resetReceivedStatus()
      onCloseConnection()
    }
  }

  private fun scheduleCheckPongTimeout() {
    if (pongTimeoutMs == null) {
      return
    }
    val lastPing = this.lastPing

    if (lastPing <= 0) {
      pongTimeoutTask?.checkAndCancel()
      pongTimeoutTask = executor.schedule(
        this::onCheckPongTimeout,
        pongTimeoutMs,
        TimeUnit.MILLISECONDS
      )
    } else {
      val currTime = getCurrTime()
      val leftTimeoutMs = pongTimeoutMs - (currTime - lastPing)

      if (leftTimeoutMs <= 0) {
        resetReceivedStatus()
        onCloseConnection()
      } else {
        pongTimeoutTask?.checkAndCancel()
        pongTimeoutTask = executor.schedule(
          this::onCheckPongTimeout,
          leftTimeoutMs,
          TimeUnit.MILLISECONDS
        )
      }
    }
  }

  private fun onCheckPongTimeout() {
    if (!isConnectionActive) {
      return
    }
    val checkMs = pongTimeoutMs ?: return
    val currTime = getCurrTime()
    val lastPing = this.lastPing

    if (lastPing > 0 && currTime - lastPing >= checkMs) {
      resetReceivedStatus()
      onCloseConnection()
    }
  }

  /* -- check timeout :end -- */

  private fun resetReceivedStatus() {
    isConnectionActive = false
    lastReceivedTime = 0L

    pingTask?.checkAndCancel()
    pingTask = null

    receiveTimeoutTask?.checkAndCancel()
    receiveTimeoutTask = null

    pongTimeoutTask?.checkAndCancel()
    pongTimeoutTask = null

    lastPing = 0L
  }

  private fun getCurrTime(): Long {
    return System.currentTimeMillis()
  }

  private fun Duration.toVerifiedMsOrNull(): Long? {
    return this
      .takeIf { !it.isZero && !it.isNegative }
      ?.toMillis()
  }
}