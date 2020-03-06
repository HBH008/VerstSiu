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
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicInteger

/**
 * Channel executors
 *
 * @author verstsiu created at 2020-03-04 21:27
 */
object ChannelExecutors {

  private val threadSize = Runtime.getRuntime().availableProcessors()
  private var threadIndex = AtomicInteger()

  private val executors = List(threadSize) {
    Executors.newSingleThreadExecutor()
  }

  /**
   * Shared scheduled executor
   */
  val sharedScheduledExecutor: ScheduledExecutorService = Executors.newScheduledThreadPool(threadSize)

  /**
   * Returns shared single thread executor
   */
  fun getSharedSingleThreadExecutor(): ExecutorService {
    return executors[threadIndex.getAndIncrement() % threadSize]
  }

}