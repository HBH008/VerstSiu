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