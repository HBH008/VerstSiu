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

import com.ijoic.channel.base.util.MessageQueue
import com.ijoic.messagechannel.MessageChannel

/**
 * Session handler
 *
 * @author verstsiu created at 2020-04-01 18:01
 */
internal class SessionHandler(
  private val listener: EventListener
) : MessageQueue.Handler<Any> {

  private var isActive = true

  private var state: ChannelState = ChannelState.CLOSED
  private var isRefreshPrepare = false

  override fun dispatchMessage(message: Any) {
    when (message) {
      REFRESH_PREPARE -> {
        if (!isActive) {
          checkAndCloseConnection()
        } else {
          when(state) {
            ChannelState.WAIT_RETRY -> {
              listener.resetRetryConnection()
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.CLOSED -> {
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.WAIT_PREPARE,
            ChannelState.PREPARE,
            ChannelState.CLOSING -> {
              // do nothing
            }
            ChannelState.OPEN -> {
              isRefreshPrepare = true
              state = ChannelState.CLOSING
              listener.closeConnection()
            }
          }
        }
      }
      WAIT_PREPARE -> {
        if (!isActive) {
          checkAndCloseConnection()
        } else {
          when(state) {
            ChannelState.WAIT_RETRY -> {
              listener.cancelRetryConnection()
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.CLOSED -> {
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.WAIT_PREPARE,
            ChannelState.PREPARE,
            ChannelState.OPEN,
            ChannelState.CLOSING -> {
              // do nothing
            }
          }
        }
      }
      PREPARE -> {
        if (!isActive) {
          checkAndCloseConnection()
        } else {
          when(state) {
            ChannelState.WAIT_RETRY -> {
              listener.cancelRetryConnection()
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.CLOSED -> {
              state = ChannelState.WAIT_PREPARE
              listener.onQueueRequestPrepare()
            }
            ChannelState.WAIT_PREPARE -> {
              state = ChannelState.PREPARE
              listener.openConnection()
            }
            ChannelState.PREPARE,
            ChannelState.OPEN,
            ChannelState.CLOSING -> {
              // do nothing
            }
          }
        }
      }
      CLOSE -> {
        if (!isActive) {
          listener.cancelRetryConnection()
        }

        when(state) {
          ChannelState.WAIT_RETRY -> {
            if (!isActive) {
              listener.cancelRetryConnection()
              state = ChannelState.CLOSED
              listener.onQueueRequestCancel()
            }
          }
          ChannelState.CLOSED,
          ChannelState.PREPARE,
          ChannelState.CLOSING -> {
            // do nothing
          }
          ChannelState.WAIT_PREPARE -> {
            if (!isActive) {
              state = ChannelState.CLOSED
              listener.onQueueRequestCancel()
            }
          }
          ChannelState.OPEN -> {
            state = if (listener.closeConnection()) {
              ChannelState.CLOSING
            } else {
              ChannelState.CLOSED
            }
          }
        }
      }
      CLOSE_COMPLETE -> {
        state = ChannelState.CLOSED

        if (isActive) {
          if (isRefreshPrepare) {
            isRefreshPrepare = false
            state = ChannelState.WAIT_PREPARE
            listener.onQueueRequestPrepare()
          } else {
            listener.scheduleRetryConnection()
          }
        }
        listener.onConnectionClosed()
      }
      is SendMessage -> if (isActive) {
        listener.cacheMessage(message.data)

        when(state) {
          ChannelState.CLOSED -> {
            state = ChannelState.WAIT_PREPARE
            listener.onQueueRequestPrepare()
          }
          ChannelState.WAIT_RETRY,
          ChannelState.WAIT_PREPARE,
          ChannelState.PREPARE,
          ChannelState.CLOSING -> {
            // do nothing
          }
          ChannelState.OPEN -> {
            listener.writeAllMessages()
          }
        }
      }
      is PingMessage -> {
        if (isActive && state == ChannelState.OPEN) {
          listener.writeMessage(message.data)
        }
      }
      is ConnectionComplete -> {
        listener.resetRetryConnection()
        state = ChannelState.OPEN
        isRefreshPrepare = false

        if (!isActive) {
          state = if (listener.closeConnection()) {
            ChannelState.CLOSING
          } else {
            ChannelState.CLOSED
          }
        } else {
          listener.onConnectionOpen(message.writer)
        }
      }
      is ConnectionFailure -> {
        state = ChannelState.CLOSED
        isRefreshPrepare = false

        if (isActive) {
          listener.scheduleRetryConnection()
        }
        listener.onConnectionFailure(message.error)
      }
      is AddChannelListener -> {
        if (listener.addChannelListener(message.data)) {
          if (isActive && state == ChannelState.OPEN) {
            listener.onChannelActive(message.data)
          }
        }
      }
      is RemoveChannelListener -> {
        if (listener.removeChannelListener(message.data)) {
          listener.onChannelInactive(message.data)
        }
      }
    }
  }

  private fun checkAndCloseConnection() {
    when(state) {
      ChannelState.WAIT_RETRY -> {
        listener.cancelRetryConnection()
        state = ChannelState.CLOSED
        listener.onQueueRequestCancel()
      }
      ChannelState.CLOSED,
      ChannelState.PREPARE,
      ChannelState.CLOSING -> {
        // do nothing
      }
      ChannelState.WAIT_PREPARE -> {
        state = ChannelState.CLOSED
        listener.onQueueRequestCancel()
      }
      ChannelState.OPEN -> {
        state = if (!listener.closeConnection()) {
          ChannelState.CLOSED
        } else {
          ChannelState.CLOSING
        }
      }
    }
  }

  fun destroy() {
    isActive = false
  }

  /**
   * Event listener
   */
  interface EventListener {
    /**
     * Queue request cancel
     */
    fun onQueueRequestPrepare()

    /**
     * Queue request cancel
     */
    fun onQueueRequestCancel()

    /**
     * Connection open
     */
    fun onConnectionOpen(writer: MessageChannel.ChannelWriter)

    /**
     * Connection failure
     */
    fun onConnectionFailure(error: Throwable)

    /**
     * Connection closed
     */
    fun onConnectionClosed()

    /**
     * Channel active
     */
    fun onChannelActive(listener: MessageChannel.ChannelListener)

    /**
     * Channel inactive
     */
    fun onChannelInactive(listener: MessageChannel.ChannelListener)

    /**
     * Destroy
     */
    fun onDestroy() {}

    /**
     * Open connection
     */
    fun openConnection()

    /**
     * Close connection
     */
    fun closeConnection(): Boolean

    /**
     * Schedule retry connection
     */
    fun scheduleRetryConnection()

    /**
     * Cancel retry connection
     */
    fun cancelRetryConnection()

    /**
     * Reset retry connection
     */
    fun resetRetryConnection()

    /**
     * Cache [message]
     */
    fun cacheMessage(message: Any)

    /**
     * Write [message]
     */
    fun writeMessage(message: Any)

    /**
     * Write all messages
     */
    fun writeAllMessages()

    /**
     * Add channel [listener]
     */
    fun addChannelListener(listener: MessageChannel.ChannelListener): Boolean

    /**
     * Remove channel [listener]
     */
    fun removeChannelListener(listener: MessageChannel.ChannelListener): Boolean
  }

  /**
   * Send message
   */
  data class SendMessage(
    val data: Any
  )

  /**
   * Ping message
   */
  data class PingMessage(
    val data: Any
  )

  /**
   * Connection complete
   */
  data class ConnectionComplete(
    val writer: MessageChannel.ChannelWriter
  )

  /**
   * Connection failure
   */
  data class ConnectionFailure(
    val error: Throwable
  )

  /**
   * Add channel listener
   */
  data class AddChannelListener(
    val data: MessageChannel.ChannelListener
  )

  /**
   * Remove channel listener
   */
  data class RemoveChannelListener(
    val data: MessageChannel.ChannelListener
  )

  companion object {
    const val REFRESH_PREPARE = "restart_prepare"
    const val WAIT_PREPARE = "wait_prepare"
    const val PREPARE = "prepare"
    const val CLOSE = "close"
    const val CLOSE_COMPLETE = "close_complete"
  }
}