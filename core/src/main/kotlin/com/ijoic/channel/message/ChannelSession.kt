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
package com.ijoic.channel.message

import com.ijoic.channel.base.util.ChannelExecutors
import com.ijoic.channel.base.util.MessageQueue
import com.ijoic.messagechannel.MessageChannel
import com.ijoic.messagechannel.options.PingOptions
import com.ijoic.messagechannel.options.RetryOptions
import com.ijoic.messagechannel.output.LogOutput
import com.ijoic.messagechannel.util.PingManager
import com.ijoic.messagechannel.util.RetryManager
import com.ijoic.messagechannel.util.checkAndCancel
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * Channel session
 *
 * @author verstsiu created at 2020-03-04 14:58
 */
internal class ChannelSession(
  private val logOutput: LogOutput,
  private val onOpen: (() -> Unit)? = null,
  private val onClosed: (() -> Unit)? = null,
  private val onMessage: ((Long, Any) -> Unit)? = null,
  private val onError: ((Throwable) -> Unit)? = null,
  private val handler: MessageChannel.PrepareHandler,
  pingOptions: PingOptions? = null,
  retryOptions: RetryOptions? = null
) {

  /**
   * Active status
   */
  var isActive = true
    private set

  private var isChannelPrepare = false
  private var isChannelReady = false
  private var isRefreshPrepare = false

  private val scheduledExecutor = ChannelExecutors.sharedScheduledExecutor
  private val pingManager = PingManager(
    scheduledExecutor,
    pingOptions ?: PingOptions(enabled = false),
    { notifyPingRequired(it) },
    { notifyRestartConnection() }
  )

  private val stateListener = object : MessageChannel.StateListener {
    override fun onConnectionComplete(writer: MessageChannel.ChannelWriter) {
      writer.logOutput = logOutput
      writer.onError = onError
      messageQueue.submit(ConnectionComplete(writer))
    }

    override fun onConnectionFailure(t: Throwable) {
      messageQueue.submit(ConnectionFailure(t))
    }

    override fun onConnectionClosed() {
      messageQueue.submit(CLOSE_COMPLETE)
    }

    override fun onMessageReceived(receiveTime: Long, message: Any) {
      if (pingManager.checkPongMessage(message)) {
        pingManager.onReceivedMessage(isPongMessage = true)
        logOutput.trace("receive pong message: $message")
      } else {
        val pongMessage = pingOptions?.mapPongMessage?.invoke(message)

        if (pongMessage != null) {
          notifyPingRequired(pongMessage)
        } else {
          pingManager.onReceivedMessage(isPongMessage = false)
          onMessage?.invoke(receiveTime, message)
        }
      }
    }
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    messageQueue.submit(SendMessage(message))
  }

  fun refresh() {
    notifyRestartConnection()
  }

  fun destroy() {
    isActive = false
    messageQueue.submit(CLOSE)
    messageQueue.destroy()
  }

  /* -- task :begin -- */

  private val messageQueue = MessageQueue(
    ChannelExecutors.getSharedSingleThreadExecutor(),
    this::dispatchMessage
  )

  private val messages = mutableListOf<Any>()
  private var activeWriter: MessageChannel.ChannelWriter? = null

  init {
    handler.logOutput = logOutput
    messageQueue.submit(PREPARE)
  }

  private fun dispatchMessage(message: Any) {
    when (message) {
      RESET_PREPARE -> if (isActive && !isChannelReady && !isChannelPrepare) {
        isChannelPrepare = true
        onResetRetryConnection()
        onPrepareConnection()
      }
      RESTART_PREPARE -> if (isActive && isChannelReady) {
        isChannelReady = false
        isRefreshPrepare = true
        onCloseConnection()
      }
      PREPARE -> {
        if (isActive && !isChannelReady && !isChannelPrepare) {
          isChannelPrepare = true
          onPrepareConnection()
        } else {
          logOutput.info("prepare cancelled: active - $isActive, ready - $isChannelReady, prepare - $isChannelPrepare")
        }
      }
      CLOSE -> if (!isActive) {
        onResetRetryConnection()

        if (isChannelReady) {
          isChannelReady = false
          onCloseConnection()
        }
      }
      CLOSE_COMPLETE -> {
        isChannelReady = false
        isChannelPrepare = false
        onClosed?.invoke()
        pingManager.onConnectionClosed()

        if (isActive) {
          if (isRefreshPrepare) {
            isRefreshPrepare = false
            isChannelPrepare = true
            logOutput.info("closed to refresh")
            onPrepareConnection()
          } else {
            logOutput.info("closed to schedule retry")
            onScheduleRetryConnection()
          }
        } else {
          logOutput.info("closed with channel inactive")
        }
        listeners.forEach { it.onChannelInactive() }
      }
      is SendMessage -> if (isActive) {
        messages.add(message.data)

        if (isChannelReady) {
          val writer = activeWriter ?: return
          sendMessagesAll(writer)
        } else if (!isChannelPrepare) {
          isChannelPrepare = true
          onResetRetryConnection()
          onPrepareConnection()
        }
      }
      is PingMessage -> {
        if (isActive && isChannelReady) {
          val writer = activeWriter ?: return
          writer.write(message.data)
        } else {
          logOutput.trace("ping cancelled: ${message.data}")
        }
      }
      is ConnectionComplete -> {
        onResetRetryConnection()
        onOpen?.invoke()
        activeWriter = message.writer
        isChannelReady = true
        isChannelPrepare = false
        isRefreshPrepare = false
        sendMessagesAll(message.writer)
        pingManager.onConnectionComplete()

        if (!isActive) {
          isChannelReady = false
          onCloseConnection()
        } else {
          listeners.forEach { it.onChannelActive(message.writer) }
        }
      }
      is ConnectionFailure -> {
        activeWriter = null
        isChannelReady = false
        isChannelPrepare = false
        isRefreshPrepare = false
        onError?.invoke(message.error)

        if (isActive) {
          pingManager.onConnectionFailure()
          onScheduleRetryConnection()
        } else {
          logOutput.info("retry cancelled: channel inactive")
        }
        listeners.forEach { it.onChannelInactive() }
      }
      is AddListener -> {
        val changed = listeners.add(message.data)

        if (changed) {
          message.data.bind(logOutput)

          if (isActive && isChannelReady) {
            val writer = activeWriter ?: return
            message.data.onChannelActive(writer)
          }
        }
      }
      is RemoveListener -> {
        val changed = listeners.remove(message.data)

        if (changed) {
          message.data.onChannelInactive()
        }
      }
    }
  }

  /**
   * Prepare connection
   */
  private fun onPrepareConnection() {
    handler.onPrepareConnection(stateListener)
  }

  /**
   * Close connection
   */
  private fun onCloseConnection() {
    val writer = activeWriter ?: return
    activeWriter = null

    try {
      writer.close()
    } catch (e: Exception) {
      onError?.invoke(e)
    }
  }

  private fun notifyPingRequired(message: Any) {
    messageQueue.submit(PingMessage(message))
  }

  private fun notifyPrepareConnection() {
    messageQueue.submit(PREPARE)
  }

  private fun notifyRestartConnection() {
    messageQueue.submit(RESTART_PREPARE)
  }

  /**
   * Send message
   */
  private data class SendMessage(
    val data: Any
  )

  /**
   * Ping message
   */
  private data class PingMessage(
    val data: Any
  )

  /**
   * Connection complete
   */
  private data class ConnectionComplete(
    val writer: MessageChannel.ChannelWriter
  )

  /**
   * Connection failure
   */
  private data class ConnectionFailure(
    val error: Throwable
  )

  /* -- task :end -- */

  /* -- retry :begin -- */

  private val retryManager = RetryManager(retryOptions ?: RetryOptions())
  private var retryTask: Future<*>? = null

  private fun onScheduleRetryConnection() {
    clearRetryTask()

    if (!requiresConnectionActive()) {
      logOutput.info("schedule retry cancelled, channel active: $isActive, messages: ${messages.size}")
      return
    }
    val duration = retryManager.nextInterval() ?: return
    retryTask = scheduledExecutor.schedule(this::notifyPrepareConnection, duration.toMillis(), TimeUnit.MILLISECONDS)
    logOutput.info("schedule retry prepare: ${duration.toMillis()} ms")
  }

  private fun onResetRetryConnection() {
    clearRetryTask()
    retryManager.reset()
  }

  private fun clearRetryTask() {
    val task = retryTask ?: return
    retryTask = null
    task.checkAndCancel()
  }

  private fun requiresConnectionActive(): Boolean {
    return when {
      !isActive -> false
      retryManager.ignoreMessageSize || messages.isNotEmpty() -> true
      else -> listeners.any { it.requiresConnectionActive() }
    }
  }

  /* -- retry :end -- */

  /* -- event :begin -- */

  private val listeners = mutableSetOf<MessageChannel.ChannelListener>()

  /**
   * Add channel [listener]
   */
  fun addChannelListener(listener: MessageChannel.ChannelListener) {
    messageQueue.submit(AddListener(listener))
  }

  /**
   * Remove channel [listener]
   */
  fun removeChannelListener(listener: MessageChannel.ChannelListener) {
    messageQueue.submit(RemoveListener(listener))
  }

  /**
   * Add listener
   */
  private data class AddListener(
    val data: MessageChannel.ChannelListener
  )

  /**
   * Remove listener
   */
  private data class RemoveListener(
    val data: MessageChannel.ChannelListener
  )

  /* -- event :end -- */

  private fun sendMessagesAll(writer: MessageChannel.ChannelWriter) {
    if (messages.isEmpty()) {
      return
    }
    messages.forEach {
      writer.write(it)
    }
    messages.clear()
  }

  companion object {
    private const val RESET_PREPARE = "reset_prepare"
    private const val RESTART_PREPARE = "restart_prepare"
    private const val PREPARE = "prepare"
    private const val CLOSE = "close"
    private const val CLOSE_COMPLETE = "close_complete"
  }

}