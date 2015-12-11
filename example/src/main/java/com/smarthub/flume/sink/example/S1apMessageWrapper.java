package com.smarthub.flume.sink.example;

import com.smarthub.flume.sink.MessageWrapper;

public class S1apMessageWrapper implements MessageWrapper {

	private String timestamp;
	private String misisdn;
	private String type;
	private double lat;
	private double lon;

	@Override
	public MessageWrapper wrap(String msg) {

		return this;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getMisisdn() {
		return misisdn;
	}

	public void setMisisdn(String misisdn) {
		this.misisdn = misisdn;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		this.lat = lat;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

}
