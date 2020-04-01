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
package com.ijoic.channel.message

import com.ijoic.channel.base.executor.ScheduledExecutorPool
import com.ijoic.channel.base.executor.SingleThreadExecutorPool
import com.ijoic.channel.base.util.MessageQueue
import com.ijoic.channel.base.util.RateLimiterPool
import com.ijoic.messagechannel.MessageChannel
import com.ijoic.messagechannel.options.PingOptions
import com.ijoic.messagechannel.options.RetryOptions
import com.ijoic.messagechannel.output.LogOutput
import com.ijoic.messagechannel.util.PingManager
import com.ijoic.messagechannel.util.RetryManager
import com.ijoic.messagechannel.util.checkAndCancel
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * Channel session
 *
 * @author verstsiu created at 2020-03-04 14:58
 */
internal class ChannelSession(
  private val logOutput: LogOutput,
  private val onOpen: (() -> Unit)? = null,
  private val onClosed: (() -> Unit)? = null,
  private val onMessage: ((Long, Any) -> Unit)? = null,
  private val onError: ((Throwable) -> Unit)? = null,
  private val handler: MessageChannel.PrepareHandler,
  private val name: String,
  prepareMs: Long,
  pingOptions: PingOptions? = null,
  retryOptions: RetryOptions? = null
) {

  /**
   * Active status
   */
  var isActive = true
    private set

  private val scheduledExecutor = ScheduledExecutorPool.obtain(this)
  private val pingManager = PingManager(
    scheduledExecutor,
    pingOptions ?: PingOptions(enabled = false),
    { notifyPingRequired(it) },
    { notifyRestartConnection() }
  )

  private var messageQueue: MessageQueue<Any>? = null
  private val sessionHandler = SessionHandler(
    object : SessionHandler.EventListener {

      private val limiter = RateLimiterPool.obtainRateLimiter(this, name, prepareMs)

      private val messages = mutableListOf<Any>()
      private var activeWriter: MessageChannel.ChannelWriter? = null

      private val retryManager = RetryManager(retryOptions ?: RetryOptions())
      private var retryTask: Future<*>? = null

      private val listeners = mutableSetOf<MessageChannel.ChannelListener>()

      override fun onQueueRequestPrepare() {
        limiter.addRequest(this, this::notifyPrepareConnection)
      }

      override fun onQueueRequestCancel() {
        limiter.cancelRequest(this)
      }

      override fun onConnectionOpen(writer: MessageChannel.ChannelWriter) {
        activeWriter = writer
        onOpen?.invoke()
        pingManager.onConnectionComplete()
        sendMessagesAll(writer)
        listeners.forEach { it.onChannelActive(writer) }
      }

      override fun onConnectionFailure(error: Throwable) {
        activeWriter = null
        onError?.invoke(error)
        pingManager.onConnectionFailure()
        listeners.forEach { it.onChannelInactive() }
      }

      override fun onConnectionClosed() {
        onClosed?.invoke()
        pingManager.onConnectionClosed()
        listeners.forEach { it.onChannelInactive() }
      }

      override fun onChannelActive(listener: MessageChannel.ChannelListener) {
        val writer = activeWriter ?: return
        listener.onChannelActive(writer)
      }

      override fun onChannelInactive(listener: MessageChannel.ChannelListener) {
        listener.onChannelInactive()
      }

      override fun onDestroy() {
        RateLimiterPool.releaseRateLimiter(this, name)
      }

      override fun openConnection() {
        handler.onPrepareConnection(stateListener)
      }

      override fun closeConnection(): Boolean {
        val writer = activeWriter ?: return false
        activeWriter = null

        try {
          writer.close()
        } catch (e: Exception) {
          onError?.invoke(e)
        }
        return true
      }

      override fun scheduleRetryConnection() {
        cancelRetryConnection()

        if (!requiresConnectionActive()) {
          logOutput.info("schedule retry cancelled, channel active: $isActive, messages: ${messages.size}")
          return
        }
        val duration = retryManager.nextInterval() ?: return
        retryTask = scheduledExecutor.schedule(this::notifyWaitPrepareConnection, duration.toMillis(), TimeUnit.MILLISECONDS)
        logOutput.info("schedule retry prepare: ${duration.toMillis()} ms")
      }

      private fun requiresConnectionActive(): Boolean {
        return when {
          !isActive -> false
          retryManager.ignoreMessageSize || messages.isNotEmpty() -> true
          else -> listeners.any { it.requiresConnectionActive() }
        }
      }

      override fun cancelRetryConnection() {
        val task = retryTask ?: return
        retryTask = null
        task.checkAndCancel()
      }

      override fun resetRetryConnection() {
        cancelRetryConnection()
        retryManager.reset()
      }

      override fun cacheMessage(message: Any) {
        messages.add(message)
      }

      override fun writeMessage(message: Any) {
        val writer = activeWriter ?: return
        writer.write(message)
      }

      override fun writeAllMessages() {
        val writer = activeWriter ?: return
        sendMessagesAll(writer)
      }

      override fun addChannelListener(listener: MessageChannel.ChannelListener): Boolean {
        val changed = listeners.add(listener)

        if (changed) {
          listener.bind(logOutput)
        }
        return changed
      }

      override fun removeChannelListener(listener: MessageChannel.ChannelListener): Boolean {
        return listeners.remove(listener)
      }

      private fun sendMessagesAll(writer: MessageChannel.ChannelWriter) {
        if (messages.isEmpty()) {
          return
        }
        messages.forEach {
          writer.write(it)
        }
        messages.clear()
      }

      private fun notifyWaitPrepareConnection() {
        messageQueue?.submit(SessionHandler.WAIT_PREPARE)
      }

      private fun notifyPrepareConnection() {
        messageQueue?.submit(SessionHandler.PREPARE)
      }
    }
  )

  private val stateListener = object : MessageChannel.StateListener {
    override fun onConnectionComplete(writer: MessageChannel.ChannelWriter) {
      writer.logOutput = logOutput
      writer.onError = onError
      messageQueue?.submit(SessionHandler.ConnectionComplete(writer))
    }

    override fun onConnectionFailure(t: Throwable) {
      messageQueue?.submit(SessionHandler.ConnectionFailure(t))
    }

    override fun onConnectionClosed() {
      messageQueue?.submit(SessionHandler.CLOSE_COMPLETE)
    }

    override fun onMessageReceived(receiveTime: Long, message: Any) {
      if (pingManager.checkPongMessage(message)) {
        pingManager.onReceivedMessage(isPongMessage = true)
        logOutput.trace("receive pong message: $message")
      } else {
        val pongMessage = pingOptions?.mapPongMessage?.invoke(message)

        if (pongMessage != null) {
          notifyPingRequired(pongMessage)
        } else {
          pingManager.onReceivedMessage(isPongMessage = false)
          onMessage?.invoke(receiveTime, message)
        }
      }
    }
  }

  init {
    handler.logOutput = logOutput
    messageQueue = MessageQueue(
      SingleThreadExecutorPool.obtain(this),
      sessionHandler
    )
    messageQueue?.submit(SessionHandler.WAIT_PREPARE)
  }

  /**
   * Send [message]
   */
  fun send(message: Any) {
    messageQueue?.submit(SessionHandler.SendMessage(message))
  }

  fun refresh() {
    notifyRestartConnection()
  }

  fun destroy() {
    isActive = false
    sessionHandler.destroy()
    messageQueue?.submit(SessionHandler.CLOSE)
    messageQueue?.destroy()
    SingleThreadExecutorPool.release(this)
    ScheduledExecutorPool.release(this)
  }

  private fun notifyPingRequired(message: Any) {
    messageQueue?.submit(SessionHandler.PingMessage(message))
  }

  private fun notifyRestartConnection() {
    messageQueue?.submit(SessionHandler.REFRESH_PREPARE)
  }

  /**
   * Add channel [listener]
   */
  fun addChannelListener(listener: MessageChannel.ChannelListener) {
    messageQueue?.submit(SessionHandler.AddChannelListener(listener))
  }

  /**
   * Remove channel [listener]
   */
  fun removeChannelListener(listener: MessageChannel.ChannelListener) {
    messageQueue?.submit(SessionHandler.RemoveChannelListener(listener))
  }

}