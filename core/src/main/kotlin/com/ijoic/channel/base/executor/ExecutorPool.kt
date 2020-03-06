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

/**
 * Executor pool
 *
 * @author verstsiu created at 2020-03-06 17:07
 */
internal interface ExecutorPool<EXECUTOR: ExecutorService> {
  /**
   * Returns shared single thread executor service for [host]
   */
  fun obtain(host: Any): EXECUTOR

  /**
   * Release shared single thread executor service for [host]
   */
  fun release(host: Any)
}