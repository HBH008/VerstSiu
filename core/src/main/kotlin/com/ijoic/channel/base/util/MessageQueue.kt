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
package com.ijoic.channel.base.util

import java.util.concurrent.ExecutorService

/**
 * Message queue
 *
 * @author verstsiu created at 2020-02-26 10:07
 */
internal class MessageQueue<MESSAGE: Any>(
  executor: ExecutorService,
  private val handleMessage: (MESSAGE) -> Unit
) {

  private var executorImpl: ExecutorService? = executor

  /**
   * Push [message]
   */
  fun submit(message: MESSAGE) {
    val executor = executorImpl ?: return
    executor.submit { handleMessage(message) }
  }

  /**
   * Close message queue
   */
  fun destroy() {
    executorImpl = null
  }
}