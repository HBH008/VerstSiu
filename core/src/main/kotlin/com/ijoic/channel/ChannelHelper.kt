/*
 *
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
 *
 */
package com.ijoic.channel

import com.ijoic.channel.base.RateLimiter
import java.util.concurrent.Executors

/**
 * Channel helper
 *
 * @author verstsiu created at 2020-02-29 16:45
 */
internal object ChannelHelper {

  private val sharedScheduleExecutor = Executors.newScheduledThreadPool(3)
  private val limiterMap = mutableMapOf<String, RateLimiter>()

  /**
   * Returns rate limiter of [name]
   */
  fun getRateLimiter(name: String, intervalMs: Long): RateLimiter {
    synchronized(limiterMap) {
      return limiterMap.getOrPut(name) { RateLimiter(sharedScheduleExecutor, intervalMs) }
    }
  }
}