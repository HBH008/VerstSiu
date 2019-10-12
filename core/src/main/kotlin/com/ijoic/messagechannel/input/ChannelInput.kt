package com.ijoic.messagechannel.input

import com.ijoic.messagechannel.Channel
import com.ijoic.messagechannel.ChannelListener
import com.ijoic.messagechannel.ChannelWriter
import com.ijoic.messagechannel.util.TaskQueue
import java.lang.ref.WeakReference
import java.util.concurrent.Executors

/**
 * Channel input
 *
 * @author verstsiu created at 2019-10-12 09:37
 */
abstract class ChannelInput : ChannelListener {

  private val executor = Executors.newScheduledThreadPool(1)
  private var refHost: WeakReference<Channel>? = null
  private var refWriter: WeakReference<ChannelWriter>? = null

  /**
   * Active writer
   */
  protected val activeWriter: ChannelWriter?
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

  override fun bind(host: Channel) {
    refHost = WeakReference(host)
  }

  override fun onChannelActive(writer: ChannelWriter) {
    refWriter = WeakReference(writer)
    taskQueue.execute(CHANNEL_ACTIVE)
  }

  override fun onChannelInactive() {
    taskQueue.execute(CHANNEL_INACTIVE)
  }

  /**
   * Writer active
   */
  protected abstract fun onWriterActive(writer: ChannelWriter)

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
   * Notify write error
   */
  protected fun notifyWriteError(error: Exception) {
    val host = refHost?.get() ?: return
    host.onError?.invoke(error)
  }

  companion object {
    private const val CHANNEL_ACTIVE = "channel_active"
    private const val CHANNEL_INACTIVE = "channel_inactive"
  }
}