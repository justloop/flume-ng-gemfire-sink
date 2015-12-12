package com.smarthub.flume.sink;

public class MessageTransformationException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// Parameterless Constructor
	public MessageTransformationException() {
	}

	// Constructor that accepts a message
	public MessageTransformationException(String message) {
		super(message);
	}
}