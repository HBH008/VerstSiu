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
import com.ijoic.messagechannel.output.LogOutput
import com.ijoic.messagechannel.util.PingManager
import com.ijoic.messagechannel.util.RetryManager
import com.ijoic.messagechannel.util.TaskQueue
import com.ijoic.messagechannel.util.checkAndCancel
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Message channel
 *
 * @author verstsiu created at 2019-10-10 11:23
 */
open class MessageChannel(
  name: String,
  private val handler: PrepareHandler,
  pingOptions: PingOptions? = null,
  retryOptions: RetryOptions? = null): Channel(name) {

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

  private val stateListener = object : StateListener {
    override fun onConnectionComplete(writer: ChannelWriter) {
      writer.logOutput = logOutput
      writer.onError = onError
      taskQueue.execute(ConnectionComplete(writer))
    }

    override fun onConnectionFailure(t: Throwable) {
      taskQueue.execute(ConnectionFailure(t))
    }

    override fun onConnectionClosed() {
      taskQueue.execute(CLOSE_COMPLETE)
    }

    override fun onMessageReceived(receiveTime: Long, message: Any) {
      val isPongMessage = pingManager.checkPongMessage(message)
      pingManager.onReceivedMessage(isPongMessage)

      if (!isPongMessage) {
        onMessage?.invoke(receiveTime, message)
      } else {
        logOutput.trace("receive pong message: $message")
      }
    }
  }

  init {
    handler.logOutput = logOutput
  }

  override fun prepare() {
    logOutput.info("channel prepare")
    isChannelActive = true
    taskQueue.execute(RESET_PREPARE)
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    taskQueue.execute(SendMessage(message))
  }

  override fun refresh() {
    logOutput.info("channel refresh")
    notifyRestartConnection()
  }

  override fun close() {
    logOutput.info("channel closed")
    isChannelActive = false
    taskQueue.execute(CLOSE)
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
          PREPARE -> {
            if (isChannelActive && !isChannelReady && !isChannelPrepare) {
              isChannelPrepare = true
              onPrepareConnection()
            } else {
              logOutput.info("prepare cancelled: active - $isChannelActive, ready - $isChannelReady, prepare - $isChannelPrepare")
            }
          }
          CLOSE -> if (!isChannelActive) {
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

            if (isChannelActive) {
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
          is PingMessage -> {
            if (isChannelActive && isChannelReady) {
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

            if (!isChannelActive) {
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

            if (isChannelActive) {
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
    taskQueue.execute(PingMessage(message))
  }

  private fun notifyRestartConnection() {
    taskQueue.execute(RESTART_PREPARE)
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

    if (!requiresConnectionActive()) {
      logOutput.info("schedule retry cancelled")
      return
    }
    val duration = retryManager.nextInterval() ?: return
    retryTask = taskQueue.schedule(PREPARE, duration.toMillis())
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
    if (!isChannelActive) {
      return false
    }
    if (messages.isNotEmpty()) {
      return true
    }

    return listeners.any { it.requiresConnectionActive() }
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
   * Channel listener
   */
  interface ChannelListener {
    /**
     * Bind
     */
    fun bind(logOutput: LogOutput)

    /**
     * Channel active
     */
    fun onChannelActive(writer: ChannelWriter)

    /**
     * Channel inactive
     */
    fun onChannelInactive()

    /**
     * Returns connection active require status
     */
    fun requiresConnectionActive() = false
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
    messages.forEach {
      writer.write(it)
    }
    messages.clear()
  }

  /**
   * Prepare handler
   */
  abstract class PrepareHandler {
    /**
     * Log output
     */
    var logOutput: LogOutput? = null
      internal set

    /**
     * Prepare connection
     */
    abstract fun onPrepareConnection(listener: StateListener)
  }

  /**
   * Channel writer
   */
  abstract class ChannelWriter {
    /**
     * Log output
     */
    internal var logOutput: LogOutput? = null

    internal var onError: ((Throwable) -> Unit)? = null

    /**
     * Write [message]
     */
    fun write(message: Any) {
      try {
        if (!onWriteMessage(message)) {
          logOutput?.trace("send message cancelled: $message")
        } else {
          logOutput?.trace("send message: $message")
        }
      } catch (e: Exception) {
        onError?.invoke(e)
        logOutput?.error("send message failed", e)
      }
    }

    /**
     * Close writer
     */
    fun close() {
      try {
        onClose()
      } catch (e: Exception) {
        onError?.invoke(e)
        logOutput?.error("channel write close failed", e)
      }
    }

    @Throws(Exception::class)
    abstract fun onWriteMessage(message: Any): Boolean

    @Throws(Exception::class)
    abstract fun onClose()
  }

  /**
   * State listener
   */
  interface StateListener {
    /**
     * Connection complete
     */
    fun onConnectionComplete(writer: ChannelWriter)

    /**
     * Connection failure
     */
    fun onConnectionFailure(t: Throwable)

    /**
     * Connection closed
     */
    fun onConnectionClosed()

    /**
     * Message received
     */
    fun onMessageReceived(receiveTime: Long, message: Any)
  }

  companion object {
    private const val RESET_PREPARE = "reset_prepare"
    private const val RESTART_PREPARE = "restart_prepare"
    private const val PREPARE = "prepare"
    private const val CLOSE = "close"
    private const val CLOSE_COMPLETE = "close_complete"
  }
}