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

import com.ijoic.messagechannel.MessageChannel
import com.ijoic.messagechannel.util.checkAndCancel
import java.time.Duration
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger

/**
 * Subscribe input
 *
 * @author verstsiu created at 2019-10-11 19:49
 */
class SubscribeInput<DATA: Any>(
  private val mapSubscribe: (Operation, DATA) -> Any,
  private val mapSubscribeMerge: ((Operation, Collection<DATA>) -> Any)? = null,
  private val mergeGroupSize: Int = 0,
  private val refreshMode: RefreshMode = RefreshMode.REPEAT,
  retryOnFailureDuration: Duration? = null
) : ChannelInput() {

  private val editId = AtomicInteger()

  /**
   * Add [subscribe]
   */
  fun add(subscribe: DATA) {
    logOutput?.trace("add subscribe: $subscribe")
    val editId = this.editId.incrementAndGet()

    post(Runnable {
      handler.add(subscribe, editId == this.editId.get())
    })
  }

  /**
   * Add all [subscribe]
   */
  fun addAll(subscribe: Collection<DATA>) {
    logOutput?.trace("add subscribe: ${subscribe.joinToString()}")
    val editId = this.editId.incrementAndGet()

    post(Runnable {
      handler.addAll(subscribe, editId == this.editId.get())
    })
  }

  /**
   * Remove [subscribe]
   */
  fun remove(subscribe: DATA) {
    logOutput?.trace("remove subscribe: $subscribe")
    val editId = this.editId.incrementAndGet()

    post(Runnable {
      handler.remove(subscribe, editId == this.editId.get())
    })
  }

  /**
   * Remove all [subscribe]
   */
  fun removeAll(subscribe: Collection<DATA>) {
    logOutput?.trace("remove subscribe: ${subscribe.joinToString()}")
    val editId = this.editId.incrementAndGet()

    post(Runnable {
      handler.removeAll(subscribe, editId == this.editId.get())
    })
  }

  /**
   * Refresh [subscribe]
   */
  fun refresh(subscribe: DATA) {
    logOutput?.trace("refresh subscribe: $subscribe")
    val editId = this.editId.incrementAndGet()

    post(Runnable {
      handler.refresh(subscribe, editId == this.editId.get())
    })
  }

  override fun onWriterActive(writer: MessageChannel.ChannelWriter) {
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

  private fun sendSubscribeMessages(writer: MessageChannel.ChannelWriter, messages: Set<DATA>, operation: Operation) {
    if (messages.isEmpty()) {
      return
    }

    if (mapSubscribeMerge != null && mergeGroupSize > 1) {
      messages.chunked(mergeGroupSize).forEach {
        writer.write(mapSubscribeMerge.invoke(operation, it))
      }

    } else {
      messages.forEach { writer.write(mapSubscribe(operation, it)) }
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

  /**
   * Refresh mode
   */
  enum class RefreshMode {
    /**
     * Repeat simple
     */
    REPEAT,

    /**
     * Clear and repeat subscribe
     */
    CLEAR_AND_REPEAT
  }

  /* -- handler :begin -- */

  private val activeItems = mutableSetOf<DATA>()
  private val subscribeItems = mutableSetOf<DATA>()
  private val unsubscribeItems = mutableSetOf<DATA>()

  private val failItems = mutableSetOf<DATA>()
  private val refreshItems = mutableSetOf<DATA>()

  override fun requiresConnectionActive(): Boolean {
    return subscribeItems.isNotEmpty() || activeItems.isNotEmpty() || refreshItems.isNotEmpty() || failItems.isNotEmpty()
  }

  /**
   * Active handler
   */
  private inner class ActiveHandler(private val writer: MessageChannel.ChannelWriter) : Handler<DATA> {
    override fun prepare() {
      commitSubscribeItems()
    }

    override fun add(subscribe: DATA, isCommitRequired: Boolean) {
      if (!failItems.contains(subscribe) && !refreshItems.contains(subscribe) && !activeItems.contains(subscribe)) {
        subscribeItems.add(subscribe)
        unsubscribeItems.remove(subscribe)
      }

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun addAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      val items = subscribeItems.toMutableList().apply {
        removeAll(failItems)
        removeAll(refreshItems)
      }
      unsubscribeItems.removeAll(items)

      this@SubscribeInput.subscribeItems.addAll(items)
      this@SubscribeInput.removeAll(activeItems)

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun remove(subscribe: DATA, isCommitRequired: Boolean) {
      when {
        failItems.contains(subscribe) -> {
          failItems.remove(subscribe)
        }
        refreshItems.contains(subscribe) -> {
          refreshItems.remove(subscribe)
        }
        activeItems.contains(subscribe) -> {
          unsubscribeItems.add(subscribe)
          subscribeItems.remove(subscribe)
        }
      }

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun removeAll(subscribeItems: Collection<DATA>, isCommitRequired: Boolean) {
      this@SubscribeInput.subscribeItems.removeAll(subscribeItems)
      failItems.removeAll(subscribeItems)
      refreshItems.removeAll(subscribeItems)

      unsubscribeItems.addAll(subscribeItems)
      unsubscribeItems.retainAll(activeItems)

      if (isCommitRequired) {
        commitSubscribeItems()
      }
    }

    override fun refresh(subscribe: DATA, isCommitRequired: Boolean) {
      when {
        failItems.contains(subscribe) -> {
          failItems.remove(subscribe)
          refreshItems.add(subscribe)
        }
        subscribeItems.contains(subscribe) -> {
          // do nothing
        }
        unsubscribeItems.contains(subscribe) -> {
          // do nothing
        }
        activeItems.contains(subscribe) -> {
          activeItems.remove(subscribe)
          refreshItems.add(subscribe)
        }
      }

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
      refreshItems.remove(subscribe)
      scheduleRetryTask()
    }

    override fun onFailRetry() {
      sendSubscribeMessages(writer, failItems, Operation.SUBSCRIBE)
      activeItems.addAll(failItems)
      failItems.clear()
    }

    private fun commitSubscribeItems() {
      if (refreshMode == RefreshMode.CLEAR_AND_REPEAT) {
        sendSubscribeMessages(writer, refreshItems, Operation.UNSUBSCRIBE)
      }
      sendSubscribeMessages(writer, refreshItems, Operation.SUBSCRIBE)
      sendSubscribeMessages(writer, subscribeItems, Operation.SUBSCRIBE)
      sendSubscribeMessages(writer, unsubscribeItems, Operation.UNSUBSCRIBE)

      activeItems.addAll(subscribeItems)
      activeItems.addAll(refreshItems)
      activeItems.removeAll(unsubscribeItems)
      subscribeItems.clear()
      refreshItems.clear()
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
      if (failItems.isNotEmpty()) {
        subscribeItems.addAll(failItems)
        failItems.clear()
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

    override fun refresh(subscribe: DATA, isCommitRequired: Boolean) {
      subscribeItems.add(subscribe)
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
     * Refresh [subscribe]
     */
    fun refresh(subscribe: DATA, isCommitRequired: Boolean)

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