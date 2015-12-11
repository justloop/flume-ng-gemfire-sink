package com.smarthub.flume.sink;

public interface MessageWrapper {
	/**
	 * Wrap the message into an object
	 * 
	 * @param msg
	 * @return
	 */
	public MessageWrapper wrap(String msg);
}
