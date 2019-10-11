package com.ijoic.messagechannel

import com.ijoic.messagechannel.util.TaskQueue

/**
 * Channel
 *
 * @author verstsiu created at 2019-10-10 11:23
 */
abstract class Channel {

  /**
   * Message callback
   */
  var onMessage: ((Any) -> Unit)? = null

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

  /**
   * Prepare channel
   */
  fun prepare() {
    isChannelActive = true
    taskQueue.execute(PREPARE)
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

  /* -- task :begin -- */

  private val taskQueue = TaskQueue(
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        when (message) {
          PREPARE -> if (isChannelActive && !isChannelReady && !isChannelPrepare) {
            isChannelPrepare = true
            onPrepareConnection()
          }
          CLOSE -> if (!isChannelActive && isChannelReady) {
            isChannelReady = false
            onCloseConnection()
          }
          CLOSE_COMPLETE -> if (!isChannelActive) {
            onClosed?.invoke()
          }
          is SendMessage -> if (isChannelActive) {
            messages.add(message.data)

            if (isChannelReady) {
              val writer = activeWriter ?: return
              sendMessagesAll(writer)
            } else if (!isChannelPrepare) {
              isChannelPrepare = true
              onPrepareConnection()
            }
          }
          is ConnectionComplete -> {
            onOpen?.invoke()
            activeWriter = message.writer
            isChannelReady = true
            isChannelPrepare = false
            sendMessagesAll(message.writer)

            if (!isChannelActive) {
              isChannelReady = false
              onCloseConnection()
            }
          }
          is ConnectionFailure -> {
            activeWriter = null
            isChannelReady = false
            isChannelPrepare = false
            onError?.invoke(message.error)
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

  protected fun notifyMessageReceived(message: Any) {
    onMessage?.invoke(message)
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
    private const val PREPARE = "prepare"
    private const val CLOSE = "close"
    private const val CLOSE_COMPLETE = "close_complete"
  }
}