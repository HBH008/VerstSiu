package com.ijoic.messagechannel

import com.ijoic.messagechannel.options.RetryOptions
import com.ijoic.messagechannel.util.RetryManager
import com.ijoic.messagechannel.util.TaskQueue
import java.util.concurrent.Future

/**
 * Channel
 *
 * @author verstsiu created at 2019-10-10 11:23
 */
abstract class Channel(options: RetryOptions?) {

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

  /* -- task :begin -- */

  private val taskQueue = TaskQueue(
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        when (message) {
          RESET_PREPARE -> if (isChannelActive && !isChannelReady && !isChannelPrepare) {
            isChannelPrepare = true
            onResetRetryConnection()
            onPrepareConnection()
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

            if (isChannelActive) {
              onScheduleRetryConnection()
            }
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
          is ConnectionComplete -> {
            onResetRetryConnection()
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

            if (isChannelActive) {
              onScheduleRetryConnection()
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

  /* -- retry :begin -- */

  private val retryManager = RetryManager(options ?: RetryOptions())
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

    if (!task.isDone && !task.isCancelled) {
      task.cancel(true)
    }
  }

  /* -- retry :end -- */

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
    private const val PREPARE = "prepare"
    private const val CLOSE = "close"
    private const val CLOSE_COMPLETE = "close_complete"
  }
}