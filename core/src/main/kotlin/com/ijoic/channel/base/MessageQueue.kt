/*
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
 */
package com.ijoic.channel.base

import java.util.concurrent.ExecutorService

/**
 * Message queue
 *
 * @author verstsiu created at 2020-02-26 10:07
 */
internal class MessageQueue<MESSAGE: Any>(
  private val handleMessage: (MESSAGE) -> Unit,
  private val waitMsToNextDispatch: Long = 200L
) {

  private val messages = mutableListOf<MESSAGE>()
  private var runnable: MessageRunnable<MESSAGE>? = null

  /**
   * Push [message]
   */
  fun push(message: MESSAGE) {
    synchronized(messages) {
      messages.add(message)
    }
  }

  private fun popAll(): List<MESSAGE> {
    synchronized(messages) {
      val result = messages.toList()
      messages.clear()
      return result
    }
  }

  /**
   * Setup message queue
   */
  fun setup(executor: ExecutorService? = null) {
    val runnable = MessageRunnable(
      this::popAll,
      handleMessage,
      waitMsToNextDispatch
    )

    if (executor == null) {
      Thread(runnable).start()
    } else {
      executor.submit(runnable)
    }
    this.runnable = runnable
  }

  /**
   * Close message queue
   */
  fun close() {
    val oldRunnable = runnable
    runnable = null
    oldRunnable?.close()
  }

  /**
   * Message runnable
   */
  private class MessageRunnable<MESSAGE: Any>(
    private val getMessages: () -> List<MESSAGE>,
    private val handleMessage: (MESSAGE) -> Unit,
    private val waitMs: Long
  ): Runnable {
    private var isActive = true

    override fun run() {
      while (true) {
        val items = getMessages()

        if (items.isNotEmpty()) {
          items.forEach(handleMessage)
        } else if (isActive) {
          Thread.sleep(waitMs)
        } else {
          break
        }
      }
    }

    fun close() {
      isActive = false
    }
  }
}