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
package com.ijoic.messagechannel.util

import com.ijoic.messagechannel.options.RetryOptions
import java.time.Duration

/**
 * Retry manager
 *
 * @author verstsiu created at 2019-10-11 11:29
 */
internal class RetryManager(options: RetryOptions) {

  private val isEnabled = options.enabled && options.intervals.isNotEmpty() && (options.retryAlways || options.maxRetrySize > 0)
  private val retryAlways = options.retryAlways
  private val maxRetrySize = options.maxRetrySize
  private val intervals = options.intervals

  private var isPrepare = false
  private var retryCount = 0
  private var intervalIndex = 0

  /**
   * Returns next retry interval or null
   */
  fun nextInterval(): Duration? {
    if (!isEnabled) {
      return null
    }
    if (!isPrepare) {
      isPrepare = true
      retryCount = 0
      intervalIndex = 0
    } else {
      ++retryCount
      ++intervalIndex

      if (intervalIndex >= intervals.size) {
        intervalIndex = 0
      }
      if (!retryAlways && retryCount >= maxRetrySize) {
        return null
      }
    }
    return intervals[intervalIndex]
  }

  fun reset() {
    isPrepare = false
  }
}