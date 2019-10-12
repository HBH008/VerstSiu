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

import com.ijoic.messagechannel.ChannelWriter

/**
 * Subscribe input
 *
 * @author verstsiu created at 2019-10-11 19:49
 */
class SubscribeInput<DATA: Any>(
  private val mapSubscribe: (Operation, DATA) -> Any,
  private val mapSubscribeMerge: ((Operation, Collection<DATA>) -> Any)? = null,
  private val mergeGroupSize: Int = 0
) : ChannelInput() {

  private var editId = 0

  private val activeItems = mutableSetOf<DATA>()
  private val subscribeItems = mutableSetOf<DATA>()
  private val unsubscribeItems = mutableSetOf<DATA>()

  /**
   * Add [subscribe]
   */
  fun add(subscribe: DATA) {
    val editId = this.editId++

    post(Runnable {
      val writer = activeWriter

      if (writer == null) {
        onWriterInactive()
        subscribeItems.add(subscribe)
      } else {
        unsubscribeItems.remove(subscribe)

        if (!activeItems.contains(subscribe)) {
          subscribeItems.add(subscribe)
        }
        if (editId == this.editId) {
          onWriterActive(writer)
        }
      }
    })
  }

  /**
   * Add all [subscribe]
   */
  fun addAll(subscribe: Collection<DATA>) {
    val editId = this.editId++

    post(Runnable {
      val writer = activeWriter

      if (writer == null) {
        onWriterInactive()
        subscribeItems.addAll(subscribe)
      } else {
        unsubscribeItems.removeAll(subscribe)

        subscribeItems.addAll(subscribe)
        subscribeItems.removeAll(activeItems)

        if (editId == this.editId) {
          onWriterActive(writer)
        }
      }
    })
  }

  /**
   * Remove [subscribe]
   */
  fun remove(subscribe: DATA) {
    val editId = this.editId++

    post(Runnable {
      val writer = activeWriter

      if (writer == null) {
        onWriterInactive()
        subscribeItems.remove(subscribe)
      } else {
        subscribeItems.remove(subscribe)

        if (activeItems.contains(subscribe)) {
          unsubscribeItems.add(subscribe)
        }
        if (editId == this.editId) {
          onWriterActive(writer)
        }
      }
    })
  }

  /**
   * Remove all [subscribe]
   */
  fun removeAll(subscribe: Collection<DATA>) {
    val editId = this.editId++

    post(Runnable {
      val writer = activeWriter

      if (writer == null) {
        onWriterInactive()
        subscribeItems.removeAll(subscribe)
      } else {
        subscribeItems.removeAll(subscribe)

        unsubscribeItems.addAll(subscribe)
        unsubscribeItems.retainAll(activeItems)

        if (editId == this.editId) {
          onWriterActive(writer)
        }
      }
    })
  }

  override fun onWriterActive(writer: ChannelWriter) {
    subscribeItems.removeAll(activeItems)
    unsubscribeItems.retainAll(activeItems)

    sendSubscribeMessages(writer, subscribeItems, Operation.SUBSCRIBE)
    sendSubscribeMessages(writer, unsubscribeItems, Operation.UNSUBSCRIBE)

    activeItems.addAll(subscribeItems)
    subscribeItems.clear()
    unsubscribeItems.clear()
  }

  override fun onWriterInactive() {
    if (activeItems.isNotEmpty()) {
      subscribeItems.addAll(activeItems)
      activeItems.clear()
    }
    if (unsubscribeItems.isNotEmpty()) {
      subscribeItems.removeAll(unsubscribeItems)
      unsubscribeItems.clear()
    }
  }

  private fun sendSubscribeMessages(writer: ChannelWriter, messages: Set<DATA>, operation: Operation) {
    try {
      if (mapSubscribeMerge != null && mergeGroupSize > 1) {
        messages.chunked(mergeGroupSize).forEach {
          writer.write(mapSubscribeMerge.invoke(operation, it))
        }

      } else {
        messages.forEach { writer.write(mapSubscribe(operation, it)) }
      }

    } catch (e: Exception) {
      notifyWriteError(e)
    }
  }

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

}