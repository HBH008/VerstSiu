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

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Rate limiter
 *
 * @author verstsiu created at 2020-03-06 20:35
 */
internal class RateLimiter(
  private val executor: ScheduledExecutorService,
  private val intervalMs: Long
) {

  private var requestTask: ScheduledFuture<*>? = null
  private val hosts = mutableListOf<Any>()
  private val requestMap = mutableMapOf<Any, () -> Unit>()
  private val editLock = Object()

  private var lastCompleteTime = 0L

  /**
   * Add host request
   */
  fun addRequest(host: Any, callback: () -> Unit) {
    synchronized(editLock) {
      val oldTask = requestTask

      if (oldTask == null || oldTask.isDone || oldTask.isCancelled) {
        val completeTime = lastCompleteTime
        val delayMs = when {
          completeTime <= 0 -> 0L
          else -> intervalMs.coerceAtMost(intervalMs - (System.currentTimeMillis() - completeTime))
        }

        requestTask = executor.schedule(
          this::checkoutAndPerformHostRequest,
          delayMs,
          TimeUnit.MILLISECONDS
        )
      }
      if (!hosts.contains(host)) {
        hosts.add(host)
        requestMap[host] = callback
      }
    }
  }

  private fun checkoutAndPerformHostRequest() {
    synchronized(editLock) {
      if (hosts.isEmpty()) {
        return
      }
      val host = hosts.removeAt(0)
      val callback = requestMap.remove(host) ?: return

      lastCompleteTime = System.currentTimeMillis()
      callback.invoke()

      if (hosts.isNotEmpty()) {
        requestTask = executor.schedule(
          this::checkoutAndPerformHostRequest,
          intervalMs,
          TimeUnit.MILLISECONDS
        )
      }
    }
  }

  /**
   * Cancel host request
   */
  fun cancelRequest(host: Any) {
    synchronized(editLock) {
      hosts.remove(host)
      requestMap.remove(host)

      if (hosts.isEmpty()) {
        clearRequestTask()
      }
    }
  }

  private fun clearRequestTask() {
    val oldTask = requestTask ?: return
    requestTask = null

    if (!oldTask.isDone && !oldTask.isCancelled) {
      oldTask.cancel(true)
    }
  }
}