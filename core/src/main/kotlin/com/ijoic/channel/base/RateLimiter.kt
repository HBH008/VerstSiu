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

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit

/**
 * Rate limiter
 *
 * @author verstsiu created at 2020-02-29 15:55
 */
internal class RateLimiter(
  private val executor: ScheduledExecutorService,
  private val intervalMs: Long
) {

  private var requestTask: ScheduledFuture<*>? = null
  private var requestTime = 0L
  private var commitRequestTime = 0L

  private val requestItems = mutableListOf<Pair<Any, () -> Unit>>()

  private val editLock = Object()

  /**
   * Schedule next request
   */
  fun scheduleNextRequest(host: Any, onRequest: () -> Unit) {
    synchronized(editLock) {
      if (!requestItems.any { it.first == host }) {
        requestItems.add(host to onRequest)
      }
      scheduleNextRequest()
    }
  }

  private fun scheduleNextRequest() {
    val oldTask = requestTask
    val commitRequestTime = this.commitRequestTime

    if (oldTask == null || oldTask.isDone || oldTask.isCancelled) {
      val currTime = System.currentTimeMillis()
      val delayMs = when(commitRequestTime) {
        0L -> 0L
        else -> (intervalMs - (currTime - commitRequestTime)).coerceAtLeast(0L)
      }
      requestTask = executor.schedule(this::checkoutAndPerformRequestItem, delayMs, TimeUnit.MILLISECONDS)
      requestTime = currTime
    }
  }

  private fun checkoutAndPerformRequestItem() {
    val onRequest = checkoutRequestItem() ?: return

    synchronized(editLock) {
      commitRequestTime = requestTime

      if (requestItems.isNotEmpty()) {
        scheduleNextRequest()
      }
    }
    onRequest.invoke()
  }

  private fun checkoutRequestItem(): (() -> Unit)? {
    synchronized(editLock) {
      return requestItems.firstOrNull()?.second
    }
  }

  /**
   * Cancel next request
   */
  fun cancelNextRequest(host: Any) {
    synchronized(editLock) {
      requestItems.removeIf { it.first == host }
    }
  }

}