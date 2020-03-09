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

import com.ijoic.channel.message.ChannelSession
import com.ijoic.messagechannel.options.PingOptions
import com.ijoic.messagechannel.options.RetryOptions
import com.ijoic.messagechannel.output.LogOutput

/**
 * Message channel
 *
 * @author verstsiu created at 2019-10-10 11:23
 */
open class MessageChannel(
  private val name: String,
  private val prepareMs: Long,
  private val handler: PrepareHandler,
  private val pingOptions: PingOptions? = null,
  private val retryOptions: RetryOptions? = null
): Channel(name) {

  /**
   * Open callback
   */
  var onOpen: (() -> Unit)? = null

  /**
   * Closed callback
   */
  var onClosed: (() -> Unit)? = null

  override fun prepare() {
    logOutput.info("channel prepare")
    prepareSession()
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    val session = getActiveSession() ?: return
    session.send(message)
  }

  override fun refresh() {
    logOutput.info("channel refresh")
    val session = getActiveSession() ?: return
    session.refresh()
  }

  override fun close() {
    logOutput.info("channel closed")
    destroyActiveSession()
  }

  /* -- session :begin -- */

  private var sessionImpl: ChannelSession? = null
  private val sessionLock = Object()

  private fun prepareSession(): ChannelSession {
    synchronized(sessionLock) {
      var session = sessionImpl

      if (session == null || !session.isActive) {
        session = ChannelSession(logOutput, onOpen, onClosed, onMessage, onError, handler, name, prepareMs, pingOptions, retryOptions)
        listeners.forEach { session.addChannelListener(it) }
        sessionImpl = session
      }
      return session
    }
  }

  private fun getActiveSession(): ChannelSession? {
    return sessionImpl?.takeIf { it.isActive }
  }

  private fun destroyActiveSession() {
    synchronized(sessionLock) {
      val session = sessionImpl ?: return
      sessionImpl = null

      if (session.isActive) {
        session.destroy()
      }
    }
  }

  /* -- session :end -- */

  /* -- event :begin -- */

  private val listeners = mutableSetOf<ChannelListener>()

  /**
   * Add channel [listener]
   */
  fun addChannelListener(listener: ChannelListener) {
    listeners.add(listener)

    val session = getActiveSession() ?: return
    session.addChannelListener(listener)
  }

  /**
   * Remove channel [listener]
   */
  fun removeChannelListener(listener: ChannelListener) {
    listeners.remove(listener)

    val session = getActiveSession() ?: return
    session.removeChannelListener(listener)
  }

  /**
   * Channel listener
   */
  interface ChannelListener {
    /**
     * Bind
     */
    fun bind(logOutput: LogOutput)

    /**
     * Channel active
     */
    fun onChannelActive(writer: ChannelWriter)

    /**
     * Channel inactive
     */
    fun onChannelInactive()

    /**
     * Returns connection active require status
     */
    fun requiresConnectionActive() = false
  }

  /* -- event :end -- */

  /**
   * Prepare handler
   */
  abstract class PrepareHandler {
    /**
     * Log output
     */
    var logOutput: LogOutput? = null
      internal set

    /**
     * Prepare connection
     */
    abstract fun onPrepareConnection(listener: StateListener)
  }

  /**
   * Channel writer
   */
  abstract class ChannelWriter {
    /**
     * Log output
     */
    internal var logOutput: LogOutput? = null

    internal var onError: ((Throwable) -> Unit)? = null

    /**
     * Write [message]
     */
    fun write(message: Any) {
      try {
        if (!onWriteMessage(message)) {
          logOutput?.debug("send message cancelled: $message")
        } else {
          logOutput?.trace("send message: $message")
        }
      } catch (e: Exception) {
        onError?.invoke(e)
        logOutput?.error("send message failed", e)
      }
    }

    /**
     * Close writer
     */
    fun close() {
      try {
        onClose()
      } catch (e: Exception) {
        onError?.invoke(e)
        logOutput?.error("channel write close failed", e)
      }
    }

    @Throws(Exception::class)
    abstract fun onWriteMessage(message: Any): Boolean

    @Throws(Exception::class)
    abstract fun onClose()
  }

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