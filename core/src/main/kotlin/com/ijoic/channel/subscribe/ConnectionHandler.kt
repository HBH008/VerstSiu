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

/**
 * Connection handler
 *
 * @author verstsiu created at 2020-02-28 11:03
 */
interface ConnectionHandler {
  /**
   * Prepare connection
   */
  fun prepareConnection(listener: StateListener)

  /**
   * State listener
   */
  interface StateListener {
    /**
     * Connection complete
     */
    fun onConnectionComplete(writer: ChannelWriter)

    /**
     * Connection failure
     */
    fun onConnectionFailure(t: Throwable)

    /**
     * Connection closed
     */
    fun onConnectionClosed()

    /**
     * Message received
     */
    fun onMessageReceived(receiveTime: Long, message: Any)
  }
}