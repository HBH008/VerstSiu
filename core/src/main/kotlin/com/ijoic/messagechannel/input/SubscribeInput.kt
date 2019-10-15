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
import com.ijoic.messagechannel.util.checkAndCancel
import java.time.Duration
import java.util.concurrent.Future

/**
 * Subscribe input
 *
 * @author verstsiu created at 2019-10-11 19:49
 */
class SubscribeInput<DATA: Any>(
  private val mapSubscribe: (Operation, DATA) -> Any,
  private val mapSubscribeMerge: ((Operation, Collection<DATA>) -> Any)? = null,
  private val mergeGroupSize: Int = 0,
  retryOnFailureDuration: Duration? = null
) : ChannelInput() {

  private var editId = 0

  /**
   * Add [subscribe]
   */
  fun add(subscribe: DATA) {
    val editId = this.editId++

    post(Runnable {
      handler.add(subscribe, editId == this.editId)
    })
  }

  /**
   * Add all [subscribe]
   */
  fun addAll(subscribe: Collection<DATA>) {
    val editId = this.editId++

    post(Runnable {
      handler.addAll(subscribe, editId == this.editId)
    })
  }

  /**
   * Remove [subscribe]
   */
  fun remove(subscribe: DATA) {
    val editId = this.editId++

    post(Runnable {
      handler.remove(subscribe, editId == this.editId)
    })
  }

  /**
   * Remove all [subscribe]
   */
  fun removeAll(subscribe: Collection<DATA>) {
    val editId = this.editId++

    post(Runnable {
      handler.removeAll(subscribe, editId == this.editId)
    })
  }

  override fun onWriterActive(writer: ChannelWriter) {
    val oldHandler = this.handler
    val handler = ActiveHandler(writer)
    this.handler = handler

    if (oldHandler != inactiveHandler) {
      inactiveHandler.prepare()
    }
    handler.prepare()
  }

  override fun onWriterInactive() {
    val oldHandler = this.handler

    if (oldHandler != inactiveHandler) {
      this.handler = inactiveHandler
      inactiveHandler.prepare()
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

  /* -- handler :begin -- */

  private val activeItems = mutableSetOf<DATA>()
  private val subscribeItems = mutableSetOf<DATA>()
  private val unsubscribeItems = mutableSetOf<DATA>()

  private val failItems = mutableSetOf<DATA>()

  /**
   * Active handler
   */
  private inner class ActiveHandler(private val writer: ChannelWriter) : Handler<DATA> {
    override fun prepare() {
      commitSubscribeItems()
    }

    override fun add(subscribe: DATA, isCommitRequired: Boolean) {
      if (!failItems.contains(subscribe) && !activeItems.contains(subscribe)) {
        subscribeItems.add(subscribe)
        unsubscribeItems.remove(subscribe)
      }

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun addAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      val items = subscribeItems.toMutableList().apply { removeAll(failItems) }
      unsubscribeItems.removeAll(items)

      this@SubscribeInput.subscribeItems.addAll(items)
      this@SubscribeInput.removeAll(activeItems)

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun remove(subscribe: DATA, isCommitRequired: Boolean) {
      if (!failItems.contains(subscribe) && activeItems.contains(subscribe)) {
        unsubscribeItems.add(subscribe)
        subscribeItems.remove(subscribe)
      }

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun removeAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      val items = subscribeItems.toMutableList().apply { removeAll(failItems) }
      this@SubscribeInput.subscribeItems.removeAll(items)

      unsubscribeItems.addAll(items)
      unsubscribeItems.retainAll(activeItems)

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun onSubscribeFailed(subscribe: DATA) {
      if (!failItems.add(subscribe)) {
        return
      }
      activeItems.remove(subscribe)
      subscribeItems.remove(subscribe)
      unsubscribeItems.remove(subscribe)
      scheduleRetryTask()
    }

    override fun onFailRetry() {
      sendSubscribeMessages(writer, failItems, Operation.SUBSCRIBE)
      activeItems.addAll(failItems)
      failItems.clear()
    }

    private fun commitSubscribeItems() {
      sendSubscribeMessages(writer, subscribeItems, Operation.SUBSCRIBE)
      sendSubscribeMessages(writer, unsubscribeItems, Operation.UNSUBSCRIBE)

      activeItems.addAll(subscribeItems)
      subscribeItems.clear()
      unsubscribeItems.clear()
    }
  }

  /**
   * Inactive handler
   */
  private val inactiveHandler = object : Handler<DATA> {
    override fun prepare() {
      if (activeItems.isNotEmpty()) {
        subscribeItems.addAll(activeItems)
        activeItems.clear()
      }
      if (unsubscribeItems.isNotEmpty()) {
        subscribeItems.removeAll(unsubscribeItems)
        unsubscribeItems.clear()
      }
      clearRetryTask()
    }

    override fun add(subscribe: DATA, isCommitRequired: Boolean) {
      subscribeItems.add(subscribe)
    }

    override fun addAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      this@SubscribeInput.subscribeItems.addAll(subscribeItems)
    }

    override fun remove(subscribe: DATA, isCommitRequired: Boolean) {
      subscribeItems.remove(subscribe)
    }

    override fun removeAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      this@SubscribeInput.subscribeItems.removeAll(subscribeItems)
    }

    override fun onSubscribeFailed(subscribe: DATA) {
      // do nothing
    }

    override fun onFailRetry() {
      // do nothing
    }
  }

  private var handler: Handler<DATA> = inactiveHandler

  /**
   * Handler
   */
  private interface Handler<DATA: Any> {
    /**
     * Prepare handler
     */
    fun prepare()

    /**
     * Add [subscribe]
     */
    fun add(subscribe: DATA, isCommitRequired: Boolean)

    /**
     * Add all [subscribeItems]
     */
    fun addAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean)

    /**
     * Remove [subscribe]
     */
    fun remove(subscribe: DATA, isCommitRequired: Boolean)

    /**
     * Remove all [subscribeItems]
     */
    fun removeAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean)

    /**
     * Subscribe failed
     */
    fun onSubscribeFailed(subscribe: DATA)

    /**
     * Fail retry
     */
    fun onFailRetry()
  }

  /* -- handler :end -- */

  /* -- state :begin -- */

  private val retryOnFailureEnabled = retryOnFailureDuration != null && retryOnFailureDuration.toMillis() >= 0
  private val retryOnFailureMs = retryOnFailureDuration?.toMillis()?.coerceAtLeast(MIN_RETRY_MS) ?: 0L

  private var retryTask: Future<*>? = null

  /**
   * Notify [subscribe] failure
   */
  fun notifySubscribeFailure(subscribe: DATA) {
    if (!retryOnFailureEnabled) {
      return
    }
    post(Runnable {
      handler.onSubscribeFailed(subscribe)
    })
  }

  private fun scheduleRetryTask() {
    val oldTask = retryTask

    if (oldTask == null || oldTask.isDone || oldTask.isCancelled) {
      retryTask = schedule(Runnable {
        handler.onFailRetry()
      }, retryOnFailureMs)
    }
  }

  private fun clearRetryTask() {
    retryTask?.checkAndCancel()
    retryTask = null
  }

  /* -- state :end -- */

  companion object {
    private const val MIN_RETRY_MS = 100L
  }

}