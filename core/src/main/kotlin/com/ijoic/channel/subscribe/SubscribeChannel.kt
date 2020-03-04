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
package com.ijoic.channel.subscribe

import com.ijoic.channel.Channel

/**
 * Subscribe channel
 *
 * @author verstsiu created at 2020-02-28 10:04
 */
open class SubscribeChannel(
  private val name: String,
  private val prepareIntervalMs: Long,
  private val handler: ConnectionHandler
) : Channel() {

  private var manager: ChannelManager? = null
  private val managerLock = Object()

  override fun setup() {
    synchronized(managerLock) {
      var manager = this.manager

      if (manager == null || !manager.isActive) {
        manager = ChannelManager(
          name,
          prepareIntervalMs,
          handler,
          { receiveTime, message ->
            onMessage?.invoke(receiveTime, message)
          }
        )
        manager.prepareConnection()
        this.manager = manager
      }
    }
  }

  override fun close() {
    synchronized(managerLock) {
      val oldManager = manager ?: return
      manager = null

      if (oldManager.isActive) {
        oldManager.destroy()
      }
    }
  }

}