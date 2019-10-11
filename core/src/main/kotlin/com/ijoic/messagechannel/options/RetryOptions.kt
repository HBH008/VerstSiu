package com.ijoic.messagechannel.options

import java.time.Duration

/**
 * Retry options
 *
 * @author verstsiu created at 2019-10-11 14:20
 */
data class RetryOptions(
  val enabled: Boolean = true,
  val retryAlways: Boolean = true,
  val maxRetrySize: Int = 0,
  val intervals: List<Duration> = listOf(
    Duration.ZERO,
    Duration.ofSeconds(1),
    Duration.ofSeconds(2),
    Duration.ofSeconds(5),
    Duration.ofSeconds(10),
    Duration.ofSeconds(30),
    Duration.ofMinutes(1)
  )
)