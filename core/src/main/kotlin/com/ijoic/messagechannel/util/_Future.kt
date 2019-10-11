package com.ijoic.messagechannel.util

import java.util.concurrent.Future

/**
 * Check and cancel current future
 *
 * @author verstsiu created at 2019-01-24 17:07
 */
fun <T> Future<T>.checkAndCancel() {
  if (!this.isDone && !this.isCancelled) {
    this.cancel(true)
  }
}