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
package com.ijoic.messagechannel.input

import com.ijoic.messagechannel.MessageChannel

/**
 * Stick input
 *
 * Used to send messages always after connection complete
 *
 * @author verstsiu created at 2020-08-03 20:25
 */
class StickInput : ChannelInput() {

  private var messages: Set<String> = emptySet()

  /**
   * Add [message]
   */
  fun add(message: String) {
    messages = messages.plus(message)
  }

  /**
   * Add [messages] all
   */
  fun addAll(messages: Collection<String>) {
    this.messages = this.messages.plus(messages)
  }

  override fun onWriterActive(writer: MessageChannel.ChannelWriter) {
    messages.forEach { writer.write(it) }
  }

  override fun onWriterInactive() {
    // do nothing
  }

}