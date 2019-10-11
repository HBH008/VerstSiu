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