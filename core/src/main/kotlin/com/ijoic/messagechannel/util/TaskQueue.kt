package com.ijoic.messagechannel.util

import java.util.concurrent.Executors

/**
 * Task queue
 *
 * @author verstsiu created at 2019-10-08 11:12
 */
internal class TaskQueue(handler: Handler) {

  private val executor by lazy { Executors.newSingleThreadExecutor() }
  private val task by lazy { HandlerTask(handler) }

  /**
   * Execute [message]
   */
  fun execute(message: Any) {
    task.insertMessage(message)

    if (task.isSubmitRequired) {
      task.prepare()
      executor.submit(task)
    }
  }

  /**
   * Handler
   */
  interface Handler {
    /**
     * Handle task [message]
     */
    fun onHandleTaskMessage(message: Any)
  }

  /**
   * Handler task
   */
  private class HandlerTask(private val handler: Handler) : Runnable {
    private val messages = mutableListOf<Any>()
    private var taskBusy = false
    private var taskPrepare = false

    /**
     * Submit required status
     */
    val isSubmitRequired: Boolean
      get() = !taskBusy && !taskPrepare

    /**
     * Insert [message]
     */
    fun insertMessage(message: Any) {
      synchronized(messages) {
        messages.add(message)
      }
    }

    /**
     * Popup message
     */
    private fun popupMessage(): Any? {
      synchronized(messages) {
        if (messages.isNotEmpty()) {
          return messages.removeAt(0)
        }
      }
      return null
    }

    /**
     * Prepare task
     */
    fun prepare() {
      taskPrepare = true
    }

    override fun run() {
      taskBusy = true
      taskPrepare = false

      while(true) {
        val message = popupMessage() ?: break
        handler.onHandleTaskMessage(message)
      }
      taskBusy = false
    }
  }
}