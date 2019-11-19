package com.ijoic.messagechannel.input

import com.ijoic.messagechannel.MessageChannel
import com.ijoic.messagechannel.output.LogOutput
import com.ijoic.messagechannel.util.TaskQueue
import java.lang.ref.WeakReference
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Channel input
 *
 * @author verstsiu created at 2019-10-12 09:37
 */
abstract class ChannelInput : MessageChannel.ChannelListener {

  private val executor = Executors.newScheduledThreadPool(1)
  private var refWriter: WeakReference<MessageChannel.ChannelWriter>? = null
  private var refLogOutput: WeakReference<LogOutput>? = null

  /**
   * Log output
   */
  protected val logOutput: LogOutput?
    get() = refLogOutput?.get()

  /**
   * Active writer
   */
  protected val activeWriter: MessageChannel.ChannelWriter?
    get() = refWriter?.get()

  private val taskQueue = TaskQueue(
    executor,
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        when (message) {
          CHANNEL_ACTIVE -> {
            val writer = refWriter?.get()

            if (writer == null) {
              onWriterInactive()
            } else {
              onWriterActive(writer)
            }
          }
          CHANNEL_INACTIVE -> {
            onWriterInactive()
          }
          is Runnable -> message.run()
        }
      }
    }
  )

  override fun bind(logOutput: LogOutput) {
    refLogOutput = WeakReference(logOutput)
  }

  override fun onChannelActive(writer: MessageChannel.ChannelWriter) {
    refWriter = WeakReference(writer)
    taskQueue.execute(CHANNEL_ACTIVE)
  }

  override fun onChannelInactive() {
    taskQueue.execute(CHANNEL_INACTIVE)
  }

  /**
   * Writer active
   */
  protected abstract fun onWriterActive(writer: MessageChannel.ChannelWriter)

  /**
   * Writer inactive
   */
  protected abstract fun onWriterInactive()

  /**
   * Post [runnable]
   */
  protected fun post(runnable: Runnable) {
    taskQueue.execute(runnable)
  }

  /**
   * Schedule [runnable] with [delayMs]
   */
  protected fun schedule(runnable: Runnable, delayMs: Long) : Future<*> {
    return taskQueue.schedule(runnable, delayMs)
  }

  companion object {
    private const val CHANNEL_ACTIVE = "channel_active"
    private const val CHANNEL_INACTIVE = "channel_inactive"
  }
}