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

import com.ijoic.channel.base.executor.ScheduledExecutorPool

/**
 * Rate limiter pool
 *
 * @author verstsiu created at 2020-03-06 21:27
 */
internal object RateLimiterPool {

  private val limiterMap = mutableMapOf<String, RateLimiter>()
  private val limiterToHostMap = mutableMapOf<RateLimiter, MutableSet<Any>>()
  private val editLock = Object()

  /**
   * Returns rate limiter of [host], [name] and [intervalMs]
   */
  fun obtainRateLimiter(host: Any, name: String, intervalMs: Long): RateLimiter {
    synchronized(editLock) {
      var limiter = limiterMap[name]

      if (limiter == null) {
        val hosts = mutableSetOf<Any>()
        val executor = ScheduledExecutorPool.obtain(hosts)
        limiter = RateLimiter(executor, intervalMs)
        limiterToHostMap[limiter] = hosts
      } else {
        val hosts = limiterToHostMap.getOrPut(limiter) { mutableSetOf() }
        hosts.add(host)
      }
      return limiter
    }
  }

  /**
   * Release rate limiter of [host] and [name]
   */
  fun releaseRateLimiter(host: Any, name: String) {
    synchronized(editLock) {
      val limiter = limiterMap[name] ?: return
      val hosts = limiterToHostMap[limiter] ?: return

      hosts.remove(host)

      if (hosts.isEmpty()) {
        limiterMap.remove(name)
        limiterToHostMap.remove(limiter)
        ScheduledExecutorPool.release(hosts)
      }
    }
  }
}