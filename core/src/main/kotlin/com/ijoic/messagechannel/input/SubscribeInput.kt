package com.ijoic.messagechannel.input

import com.ijoic.messagechannel.ChannelListener
import com.ijoic.messagechannel.ChannelWriter
import com.ijoic.messagechannel.util.TaskQueue
import java.lang.ref.WeakReference
import java.util.concurrent.Executors

/**
 * Subscribe input
 *
 * @author verstsiu created at 2019-10-11 19:49
 */
class SubscribeInput<DATA: Any>(
  private val mapSubscribe: (Operation, DATA) -> Any,
  private val mapSubscribeMerge: ((Operation, Collection<DATA>) -> Any)? = null,
  private val mergeGroupSize: Int = 0
) : ChannelListener {

  private val executor = Executors.newScheduledThreadPool(1)
  private var refWriter: WeakReference<ChannelWriter>? = null

  private var editId = 0

  private val activeItems = mutableSetOf<DATA>()
  private val subscribeItems = mutableSetOf<DATA>()
  private val unsubscribeItems = mutableSetOf<DATA>()

  /**
   * Add [subscribe]
   */
  fun add(subscribe: DATA) {
    val editId = this.editId++

    taskQueue.execute(Runnable {
      val writer = refWriter?.get()

      if (writer == null) {
        mergeAllMessagesAsSubscribe()
        subscribeItems.add(subscribe)
      } else {
        unsubscribeItems.remove(subscribe)

        if (!activeItems.contains(subscribe)) {
          subscribeItems.add(subscribe)
        }
        if (editId == this.editId) {
          sendAllMessages(writer)
        }
      }
    })
  }

  /**
   * Remove [subscribe]
   */
  fun remove(subscribe: DATA) {
    val editId = this.editId++

    taskQueue.execute(Runnable {
      val writer = refWriter?.get()

      if (writer == null) {
        mergeAllMessagesAsSubscribe()
        subscribeItems.remove(subscribe)
      } else {
        subscribeItems.remove(subscribe)

        if (activeItems.contains(subscribe)) {
          unsubscribeItems.add(subscribe)
        }
        if (editId == this.editId) {
          sendAllMessages(writer)
        }
      }
    })
  }

  override fun onChannelActive(writer: ChannelWriter) {
    refWriter = WeakReference(writer)
    taskQueue.execute(CHANNEL_ACTIVE)
  }

  override fun onChannelInactive() {
    taskQueue.execute(CHANNEL_INACTIVE)
  }

  /* -- task :begin -- */

  private val taskQueue = TaskQueue(
    executor,
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        when (message) {
          CHANNEL_ACTIVE -> {
            val writer = refWriter?.get()

            if (writer == null) {
              mergeAllMessagesAsSubscribe()
            } else {
              sendAllMessages(writer)
            }
          }
          CHANNEL_INACTIVE -> {
            mergeAllMessagesAsSubscribe()
          }
          is Runnable -> message.run()
        }
      }
    }
  )

  private fun mergeAllMessagesAsSubscribe() {
    if (activeItems.isNotEmpty()) {
      subscribeItems.addAll(activeItems)
      activeItems.clear()
    }
    if (unsubscribeItems.isNotEmpty()) {
      subscribeItems.removeAll(unsubscribeItems)
      unsubscribeItems.clear()
    }
  }

  private fun sendAllMessages(writer: ChannelWriter) {
    subscribeItems.removeAll(activeItems)
    unsubscribeItems.retainAll(activeItems)

    sendSubscribeMessages(writer, subscribeItems, Operation.SUBSCRIBE)
    sendSubscribeMessages(writer, unsubscribeItems, Operation.UNSUBSCRIBE)

    activeItems.addAll(subscribeItems)
    subscribeItems.clear()
    unsubscribeItems.clear()
  }

  private fun sendSubscribeMessages(writer: ChannelWriter, messages: Set<DATA>, operation: Operation) {
    if (mapSubscribeMerge != null && mergeGroupSize > 1) {
      messages.chunked(mergeGroupSize).forEach {
        writer.write(mapSubscribeMerge.invoke(operation, it))
      }

    } else {
      messages.forEach { writer.write(mapSubscribe(operation, it)) }
    }
  }

  /* -- task :end -- */

  /**
   * Operation
   */
  enum class Operation {
    /**
     * Subscribe
     */
    SUBSCRIBE,

    /**
     * Unsubscribe
     */
    UNSUBSCRIBE
  }

  companion object {
    private const val CHANNEL_ACTIVE = "channel_active"
    private const val CHANNEL_INACTIVE = "channel_inactive"
  }
}