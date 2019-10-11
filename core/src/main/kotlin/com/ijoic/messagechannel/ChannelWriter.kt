package com.ijoic.messagechannel

/**
 * Channel writer
 *
 * @author verstsiu created at 2019-10-10 12:46
 */
interface ChannelWriter {
  /**
   * Write [message]
   */
  @Throws(Exception::class)
  fun write(message: Any)

  /**
   * Close writer
   */
  @Throws(Exception::class)
  fun close()
}