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

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * Single thread executor pool
 *
 * @author verstsiu created at 2020-03-06 15:34
 */
internal object SingleThreadExecutorPool : ExecutorPool<ExecutorService> {

  private val processorSize = Runtime.getRuntime().availableProcessors()
  private const val MIN_SHARE_HOST_SIZE = 5

  private val hostToExecutorMap = mutableMapOf<Any, ExecutorService>()
  private val stateMap = mutableMapOf<ExecutorService, ExecutorState>()
  private val editLock = Object()

  override fun obtain(host: Any): ExecutorService {
    synchronized(editLock) {
      val oldExecutor = hostToExecutorMap[host]

      if (oldExecutor != null) {
        return oldExecutor
      }
      var minHostSize = -1
      var minHostState: ExecutorState? = null
      var matchState: ExecutorState? = null

      for ((_, state) in stateMap) {
        val hostSize = state.hosts.size

        if (hostSize < MIN_SHARE_HOST_SIZE) {
          matchState = state
          break
        }
        if (minHostSize < 0 || hostSize < minHostSize) {
          minHostSize = hostSize
          minHostState = state
        }
      }

      return when {
        matchState != null -> {
          matchState.hosts.add(host)
          hostToExecutorMap[host] = matchState.executor
          matchState.executor
        }
        minHostState == null || stateMap.values.size < processorSize -> {
          val executor = Executors.newSingleThreadExecutor()
          val state = ExecutorState(executor)
          state.hosts.add(host)
          stateMap[executor] = state
          hostToExecutorMap[host] = executor
          executor
        }
        else -> {
          minHostState.hosts.add(host)
          hostToExecutorMap[host] = minHostState.executor
          minHostState.executor
        }
      }
    }
  }

  override fun release(host: Any) {
    synchronized(editLock) {
      val executor = hostToExecutorMap[host] ?: return
      hostToExecutorMap.remove(host)

      val state = stateMap[executor] ?: return
      state.hosts.remove(host)

      if (state.hosts.isEmpty()) {
        stateMap.remove(executor)
        executor.runCatching { this.shutdown() }
      }
    }
  }

  /**
   * Executor state
   */
  private class ExecutorState(
    val executor: ExecutorService,
    val hosts: MutableSet<Any> = mutableSetOf()
  )
}