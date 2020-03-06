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
package com.ijoic.channel.base.executor

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

/**
 * Scheduled executor pool
 *
 * @author verstsiu created at 2020-03-06 17:08
 */
internal object ScheduledExecutorPool : ExecutorPool<ScheduledExecutorService> {

  private val processorSize = Runtime.getRuntime().availableProcessors()

  private var executorImpl: ScheduledExecutorService? = null
  private val hosts = mutableSetOf<Any>()
  private val editLock = Object()

  override fun obtain(host: Any): ScheduledExecutorService {
    synchronized(editLock) {
      val oldExecutor = executorImpl

      if (oldExecutor != null) {
        hosts.add(host)
        return oldExecutor
      }
      val executor = Executors.newScheduledThreadPool(processorSize)
      executorImpl = executor
      hosts.add(host)
      return executor
    }
  }

  override fun release(host: Any) {
    synchronized(editLock) {
      hosts.remove(host)
      val executor = executorImpl ?: return

      if (hosts.isEmpty()) {
        executorImpl = null
        executor.runCatching { this.shutdown() }
      }
    }
  }

}