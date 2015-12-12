package com.smarthub.flume.sink.example;

/**
 * Created by cq on 20/10/14.
 */
public enum EventType {

	AIF(1), GB(2), IuCS(3), IuPS(4), S1AP(5);

	private int code;

	private EventType(int code) {
		this.code = code;
	}

	public int getCode() {
		return code;
	}

};