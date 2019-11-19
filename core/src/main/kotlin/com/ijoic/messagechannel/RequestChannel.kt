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
package com.ijoic.messagechannel

import com.ijoic.messagechannel.util.TaskQueue
import java.util.concurrent.Executors

/**
 * Request channel
 *
 * @author verstsiu created at 2019-10-15 19:01
 */
abstract class RequestChannel(name: String) : Channel(name) {

  private val executor = Executors.newScheduledThreadPool(1)

  private var isChannelActive = true
  private var isChannelPrepare = false

  private var lastRequest: Long = 0L
  private var lastCommit: Long = 0L

  override fun prepare() {
    isChannelActive = true
    lastRequest = getCurrentTime()
    taskQueue.execute(REQUEST)
  }

  override fun refresh() {
    lastRequest = getCurrentTime()
    taskQueue.execute(REQUEST)
  }

  override fun close() {
    isChannelActive = false
    lastCommit = getCurrentTime()
  }

  protected fun getCurrentTime(): Long {
    return System.currentTimeMillis()
  }

  /* -- task :begin -- */

  private val taskQueue = TaskQueue(
    executor,
    object : TaskQueue.Handler {
      override fun onHandleTaskMessage(message: Any) {
        if (message != REQUEST || !isChannelActive || lastRequest <= lastCommit) {
          return
        }
        isChannelPrepare = true

        doRequestMessage()

        lastCommit = getCurrentTime()
        isChannelPrepare = false
      }
    }
  )

  /**
   * Request message
   */
  protected abstract fun doRequestMessage()

  companion object {
    private const val REQUEST = "request"
  }
}