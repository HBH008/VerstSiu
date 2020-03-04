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
package com.ijoic.channel.subscribe

import com.ijoic.channel.ChannelHelper
import com.ijoic.channel.base.MessageQueue
import com.ijoic.channel.base.PingManager
import com.ijoic.channel.base.RetryManager
import com.ijoic.channel.base.options.PingOptions
import com.ijoic.channel.base.options.RetryOptions
import com.ijoic.messagechannel.util.checkAndCancel
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Channel manager
 *
 * @author verstsiu created at 2020-02-28 10:13
 */
internal class ChannelManager(
  name: String,
  prepareIntervalMs: Long,
  private val handler: ConnectionHandler,
  private val onMessage: (Long, Any) -> Unit,
  pingOptions: PingOptions? = null,
  retryOptions: RetryOptions? = null
) {
  /**
   * Active status
   */
  var isActive = true
    private set

  private var isDestroyed = false

  private val executor = Executors.newScheduledThreadPool(1)

  /**
   * Prepare connection
   */
  fun prepareConnection() {
    messageQueue.push(PREPARE_CONNECTION)
  }

  /**
   * Refresh connection
   */
  fun refreshConnection() {
    messageQueue.push(REFRESH_CONNECTION)
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    messageQueue.push(SendMessage(message))
  }

  /**
   * Destroy manager
   */
  fun destroy() {
    isActive = false
    messageQueue.push(CLOSE_CONNECTION)
    limiter.cancelNextRequest(this)
  }

  /* -- message queue :begin -- */

  private val messageQueue = MessageQueue(this::dispatchQueueMessage)
    .also { it.setup() }

  private fun dispatchQueueMessage(message: Any) {
    when(message) {
      PREPARE_CONNECTION -> onPrepareConnection()
      OPEN_CONNECTION -> onOpenConnection()
      CLOSE_CONNECTION -> onCloseConnection()
      REFRESH_CONNECTION -> onRefreshConnection()
      is ConnectionComplete -> onConnectionComplete(message.writer)
      is ConnectionFailure -> onConnectionFailure(message.error)
      is ConnectionClosed -> onConnectionClosed()
      is SendMessage -> {

      }
    }
  }

  private fun checkAndDestroyMessageQueue() {
    if (!isDestroyed) {
      isDestroyed = true
      destroyMessageQueue()
    }
  }

  private fun destroyMessageQueue() {
    messageQueue.close()
  }

  private fun onPrepareConnection() {
    if (!isActive) {
      checkAndDestroyConnection()
    } else {
      checkAndPrepareConnection()
    }
  }

  private fun onOpenConnection() {
    if (!isActive) {
      checkAndDestroyConnection()
    } else {
      checkAndOpenConnection()
    }
  }

  private fun onCloseConnection() {
    resetRetryConnection()

    if (!isActive) {
      checkAndDestroyConnection()
    } else {
      checkAndCloseConnection()
    }
  }

  private fun onRefreshConnection() {
    resetRetryConnection()

    if (!isActive) {
      checkAndDestroyConnection()
    } else if (state == ConnectionState.OPEN) {
      val writer = activeWriter
      activeWriter = null

      if (writer == null) {
        checkAndPrepareConnection()
      } else {
        state = ConnectionState.CLOSING
        writer.close()
      }
    }
  }

  private fun onConnectionComplete(writer: ChannelWriter) {
    resetRetryConnection()
    bindWriter(writer)
    state = ConnectionState.OPEN

    if (!isActive) {
      checkAndDestroyConnection()
    } else {
      pingManager.onConnectionComplete()
    }
  }

  private fun onConnectionFailure(error: Throwable) {
    error.printStackTrace()
    state = ConnectionState.IDLE

    if (!isActive) {
      checkAndDestroyConnection()
    } else {

    }
  }

  private fun onConnectionClosed() {
    state = ConnectionState.IDLE
    pingManager.onConnectionClosed()

//    if (isActive) {
//      onScheduleRetryConnection()
//    }
//    listeners.forEach { it.onChannelInactive() }
  }

  /**
   * Send message
   */
  private class SendMessage(
    val message: Any
  )

  /* -- message queue :end -- */

  /* -- connect manager :begin -- */

  private val limiter = ChannelHelper.getRateLimiter(name, prepareIntervalMs)
  private var state: ConnectionState = ConnectionState.IDLE

  private val stateListener = object: ConnectionHandler.StateListener {
    override fun onConnectionComplete(writer: ChannelWriter) {
      messageQueue.push(ConnectionComplete(writer))
    }

    override fun onConnectionFailure(t: Throwable) {
      messageQueue.push(ConnectionFailure(t))
    }

    override fun onConnectionClosed() {
      messageQueue.push(ConnectionClosed)
    }

    override fun onMessageReceived(receiveTime: Long, message: Any) {
      if (pingManager.checkPongMessage(message)) {
        pingManager.onReceivedMessage(isPongMessage = true)
      } else {
        val pongMessage = pingOptions?.mapPongMessage?.invoke(message)

        if (pongMessage != null) {
          messageQueue.push(PingMessage(pongMessage))
        } else {
          pingManager.onReceivedMessage(isPongMessage = false)
          onMessage.invoke(receiveTime, message)
        }
      }
    }
  }

  private fun checkAndPrepareConnection() {
    if (state != ConnectionState.IDLE) {
      return
    }
    state = ConnectionState.WAIT_PREPARE
    limiter.scheduleNextRequest(this) { messageQueue.push(OPEN_CONNECTION) }
  }

  private fun checkAndOpenConnection() {
    if (state != ConnectionState.WAIT_PREPARE) {
      return
    }
    state = ConnectionState.PREPARE
    handler.prepareConnection(stateListener)
  }

  private fun checkAndCloseConnection() {
    when(state) {
      ConnectionState.IDLE,
      ConnectionState.PREPARE,
      ConnectionState.CLOSING -> {
        // do nothing
      }
      ConnectionState.WAIT_PREPARE -> {
        state = ConnectionState.IDLE
      }
      ConnectionState.OPEN -> {
        val writer = activeWriter

        if (writer == null) {
          state = ConnectionState.IDLE
        } else {
          state = ConnectionState.CLOSING
          writer.close()
        }
      }
    }
  }

  private fun checkAndDestroyConnection() {
    when(state) {
      ConnectionState.IDLE -> {
        checkAndDestroyMessageQueue()
      }
      ConnectionState.PREPARE,
      ConnectionState.CLOSING -> {
        // do nothing
      }
      ConnectionState.WAIT_PREPARE -> {
        state = ConnectionState.IDLE
        checkAndDestroyMessageQueue()
      }
      ConnectionState.OPEN -> {
        val writer = activeWriter

        if (writer == null) {
          state = ConnectionState.IDLE
          checkAndDestroyMessageQueue()
        } else {
          state = ConnectionState.CLOSING
          writer.close()
        }
      }
    }
  }

  private fun resetConnectRequest() {
    limiter.cancelNextRequest(this)
  }

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

  /**
   * Connection closed
   */
  private object ConnectionClosed

  /**
   * Connection state
   */
  private enum class ConnectionState {
    IDLE,
    WAIT_PREPARE,
    PREPARE,
    OPEN,
    CLOSING
  }

  /* -- connect manager :end -- */

  /* -- message writer :begin -- */

  private var activeWriter: ChannelWriter? = null

  private fun bindWriter(writer: ChannelWriter) {
    activeWriter = writer
  }

  private fun releaseWriter() {
    val oldWriter = activeWriter ?: return
    activeWriter = null
    oldWriter.close()
  }

  /* -- message writer :end -- */

  /* -- ping :begin -- */

  private val pingManager = PingManager(
    executor,
    pingOptions ?: PingOptions(enabled = false),
    { messageQueue.push(PingMessage(it)) },
    { messageQueue.push(REFRESH_CONNECTION) }
  )

  /**
   * Ping message
   */
  private data class PingMessage(
    val data: Any
  )

  /* -- ping :end -- */

  /* -- retry :begin -- */

  private val retryManager = RetryManager(retryOptions ?: RetryOptions())
  private var retryTask: Future<*>? = null

  private fun resetRetryConnection() {
    clearRetryTask()
    retryManager.reset()
  }

  private fun clearRetryTask() {
    val task = retryTask ?: return
    retryTask = null
    task.checkAndCancel()
  }

  /* -- retry :end -- */

  companion object {
    private const val PREPARE_CONNECTION = "prepare_connection"
    private const val OPEN_CONNECTION = "open_connection"
    private const val CLOSE_CONNECTION = "close_connection"
    private const val REFRESH_CONNECTION = "refresh_connection"
  }
}