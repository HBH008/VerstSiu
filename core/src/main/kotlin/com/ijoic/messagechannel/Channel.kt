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
package com.ijoic.messagechannel

import com.ijoic.messagechannel.options.PingOptions
import com.ijoic.messagechannel.options.RetryOptions
import com.ijoic.messagechannel.util.PingManager
import com.ijoic.messagechannel.util.RetryManager
import com.ijoic.messagechannel.util.TaskQueue
import com.ijoic.messagechannel.util.checkAndCancel
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Channel
 *
 * @author verstsiu created at 2019-10-10 11:23
 */
abstract class Channel(
  pingOptions: PingOptions? = null,
  retryOptions: RetryOptions? = null) {

  /**
   * Message callback
   */
  var onMessage: ((Long, Any) -> Unit)? = null

  /**
   * Error callback
   */
  var onError: ((Throwable) -> Unit)? = null

  /**
   * Open callback
   */
  var onOpen: (() -> Unit)? = null

  /**
   * Closed callback
   */
  var onClosed: (() -> Unit)? = null

  private var isChannelActive = true
  private var isChannelPrepare = false
  private var isChannelReady = false
  private var isRefreshPrepare = false

  private val executor = Executors.newScheduledThreadPool(1)
  private val pingManager = PingManager(
    executor,
    pingOptions ?: PingOptions(enabled = false),
    { notifyPingRequired(it) },
    { notifyRestartConnection() }
  )

  /**
   * Prepare channel
   */
  fun prepare() {
    isChannelActive = true
    taskQueue.execute(RESET_PREPARE)
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    taskQueue.execute(SendMessage(message))
  }

  /**
   * Close channel
   */
  fun close() {
    isChannelActive = false
    taskQueue.execute(CLOSE)
  }

  /**
   * Refresh connection
   */
  fun refresh() {
    notifyRestartConnection()
  }

  /* -- task :begin -- */

  private val taskQueue = TaskQueue(
    executor,
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        when (message) {
          RESET_PREPARE -> if (isChannelActive && !isChannelReady && !isChannelPrepare) {
            isChannelPrepare = true
            onResetRetryConnection()
            onPrepareConnection()
          }
          RESTART_PREPARE -> if (isChannelActive && isChannelReady) {
            isChannelReady = false
            isRefreshPrepare = true
            onCloseConnection()
          }
          PREPARE -> if (isChannelActive && !isChannelReady && !isChannelPrepare) {
            isChannelPrepare = true
            onPrepareConnection()
          }
          CLOSE -> if (!isChannelActive) {
            onResetRetryConnection()

            if (isChannelReady) {
              isChannelReady = false
              onCloseConnection()
            }
          }
          CLOSE_COMPLETE -> {
            onClosed?.invoke()
            pingManager.onConnectionClosed()

            if (isChannelActive) {
              if (isRefreshPrepare) {
                isRefreshPrepare = false
                isChannelPrepare = true
                onPrepareConnection()
              } else {
                onScheduleRetryConnection()
              }
            }
            listeners.forEach { it.onChannelInactive() }
          }
          is SendMessage -> if (isChannelActive) {
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
          is PingMessage -> if (isChannelActive && isChannelReady) {
            val writer = activeWriter ?: return

            try {
              writer.write(message.data)
            } catch (e: Exception) {
              onError?.invoke(e)
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

            if (!isChannelActive) {
              isChannelReady = false
              onCloseConnection()
            } else {
              pingManager.onConnectionComplete()
              listeners.forEach { it.onChannelActive(message.writer) }
            }
          }
          is ConnectionFailure -> {
            activeWriter = null
            isChannelReady = false
            isChannelPrepare = false
            isRefreshPrepare = false
            onError?.invoke(message.error)

            if (isChannelActive) {
              pingManager.onConnectionFailure()
              onScheduleRetryConnection()
            }
            listeners.forEach { it.onChannelInactive() }
          }
          is AddListener -> {
            val changed = listeners.add(message.data)

            if (changed) {
              message.data.bind(this@Channel)

              if (isChannelActive && isChannelReady) {
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
    }
  )

  private val messages = mutableListOf<Any>()
  private var activeWriter: ChannelWriter? = null

  /**
   * Prepare connection
   */
  protected abstract fun onPrepareConnection()

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

  /**
   * Notify connection complete
   */
  protected fun notifyConnectionComplete(writer: ChannelWriter) {
    taskQueue.execute(ConnectionComplete(writer))
  }

  /**
   * Notify connection failure
   */
  protected fun notifyConnectionFailure(error: Throwable) {
    taskQueue.execute(ConnectionFailure(error))
  }

  protected fun notifyMessageReceived(receiveTime: Long, message: Any) {
    val isPongMessage = pingManager.checkPongMessage(message)
    pingManager.onReceivedMessage(isPongMessage)

    if (!isPongMessage) {
      onMessage?.invoke(receiveTime, message)
    }
  }

  private fun notifyPingRequired(message: Any) {
    taskQueue.execute(PingMessage(message))
  }

  private fun notifyRestartConnection() {
    taskQueue.execute(RESTART_PREPARE)
  }

  protected fun notifyConnectionClosed() {
    taskQueue.execute(CLOSE_COMPLETE)
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
    val writer: ChannelWriter
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

    val duration = retryManager.nextInterval() ?: return
    retryTask = taskQueue.schedule(PREPARE, duration.toMillis())
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

  /* -- retry :end -- */

  /* -- event :begin -- */

  private val listeners = mutableSetOf<ChannelListener>()

  /**
   * Add channel [listener]
   */
  fun addChannelListener(listener: ChannelListener) {
    taskQueue.execute(AddListener(listener))
  }

  /**
   * Remove channel [listener]
   */
  fun removeChannelListener(listener: ChannelListener) {
    taskQueue.execute(RemoveListener(listener))
  }

  /**
   * Add listener
   */
  private data class AddListener(
    val data: ChannelListener
  )

  /**
   * Remove listener
   */
  private data class RemoveListener(
    val data: ChannelListener
  )

  /* -- event :end -- */

  private fun sendMessagesAll(writer: ChannelWriter) {
    if (messages.isEmpty()) {
      return
    }
    try {
      messages.forEach { writer.write(it) }
    } catch (e: Exception) {
      onError?.invoke(e)
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