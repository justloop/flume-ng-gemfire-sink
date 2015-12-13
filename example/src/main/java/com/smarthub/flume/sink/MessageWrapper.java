package com.smarthub.flume.sink;

import java.io.Serializable;

public interface MessageWrapper extends Serializable {
	/**
	 * Wrap the message into an object
	 * 
	 * @param msg
	 * @return
	 * @throws MessageTransformationException
	 */
	public MessageWrapper wrap(String msg) throws MessageTransformationException;
}
