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
package com.ijoic.messagechannel

import com.ijoic.messagechannel.output.LogOutput
import org.apache.logging.log4j.LogManager
import java.util.concurrent.atomic.AtomicInteger

/**
 * Channel
 *
 * @author verstsiu created at 2019-10-15 18:51
 */
abstract class Channel(name: String) {

  protected val logOutput: LogOutput = DefaultLogOutput("$name - ${seedId.getAndIncrement()}")

  /**
   * Message callback
   */
  var onMessage: ((Long, Any) -> Unit)? = null

  /**
   * Error callback
   */
  var onError: ((Throwable) -> Unit)? = null

  /**
   * Prepare channel
   */
  abstract fun prepare()

  /**
   * Refresh connection
   */
  abstract fun refresh()

  /**
   * Close channel
   */
  abstract fun close()

  /**
   * Default log output
   */
  private class DefaultLogOutput(private val tag: String) : LogOutput {
    override fun trace(message: String) {
      logger.trace("[$tag] $message")
    }

    override fun info(message: String) {
      logger.info("[$tag] $message")
    }

    override fun error(message: String, t: Throwable) {
      logger.error("[$tag] $message", t)
    }
  }

  companion object {
    private val seedId = AtomicInteger()
    private val logger = LogManager.getLogger(Channel::class.java)
  }
}