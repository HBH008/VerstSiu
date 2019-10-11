package com.ijoic.messagechannel

/**
 * Channel listener
 *
 * @author verstsiu created at 2019-10-10 12:48
 */
interface ChannelListener {
  /**
   * Channel active
   */
  fun onChannelActive(writer: ChannelWriter)

  /**
   * Channel inactive
   */
  fun onChannelInactive()
}