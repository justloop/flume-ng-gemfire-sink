package com.smarthub.flume.sink.example;

import com.smarthub.flume.sink.MessageTransformationException;
import com.smarthub.flume.sink.MessageWrapper;

public class GbMessageWrapper implements MessageWrapper {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int callNumber;
	private int startTime;
	private int startTimeUSec;
	private int endTime;
	private int endTimeUSec;
	private int status;
	private int timeoutBits;
	private int transactionType;
	private int lac;
	private int cellId;
	private int callType;
	private int imsi;
	private int msisdn;
	private int imei;
	private String protocolName = "gb";

	@Override
	public MessageWrapper wrap(String msg) throws MessageTransformationException {
		String values[] = removeDoubleQuotes(msg).split(",", -1);

		// if (values[10].equalsIgnoreCase(""))
		// throw new MessageTransformationException("Unable to transform
		// message: " + msg);

		try {
			this.callNumber = tryParseInt(values[0]);
			this.startTime = tryParseInt(values[1]);
			this.startTimeUSec = tryParseInt(values[2]);
			this.endTime = tryParseInt(values[3]);
			this.endTimeUSec = tryParseInt(values[4]);
			this.status = tryParseInt(values[5]);
			this.timeoutBits = tryParseInt(values[6]);
			this.transactionType = tryParseInt(values[7]);
			this.lac = tryParseInt(values[8]);
			this.cellId = tryParseInt(values[9]);
			this.callType = tryParseInt(values[10]);
			this.imsi = tryParseInt(values[11]);
			this.msisdn = tryParseInt(values[12]);
			this.imei = tryParseInt(values[13]);
		} catch (Exception e) {
			throw new MessageTransformationException("Unable to transform message: " + e);
		}

		return this;
	}

	private int tryParseInt(String value) {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException nfe) {
			// Log exception.
			return 0;
		}
	}

	/**
	 *
	 * @param value
	 * @return
	 */
	private String removeDoubleQuotes(String value) {

		return value.replaceAll("\"", "");
	}

	public int getCallNumber() {
		return callNumber;
	}

	public void setCallNumber(int callNumber) {
		this.callNumber = callNumber;
	}

	public int getStartTime() {
		return startTime;
	}

	public void setStartTime(int startTime) {
		this.startTime = startTime;
	}

	public int getStartTimeUSec() {
		return startTimeUSec;
	}

	public void setStartTimeUSec(int startTimeUSec) {
		this.startTimeUSec = startTimeUSec;
	}

	public int getEndTime() {
		return endTime;
	}

	public void setEndTime(int endTime) {
		this.endTime = endTime;
	}

	public int getEndTimeUSec() {
		return endTimeUSec;
	}

	public void setEndTimeUSec(int endTimeUSec) {
		this.endTimeUSec = endTimeUSec;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public int getTimeoutBits() {
		return timeoutBits;
	}

	public void setTimeoutBits(int timeoutBits) {
		this.timeoutBits = timeoutBits;
	}

	public int getTransactionType() {
		return transactionType;
	}

	public void setTransactionType(int transactionType) {
		this.transactionType = transactionType;
	}

	public int getLac() {
		return lac;
	}

	public void setLac(int lac) {
		this.lac = lac;
	}

	public int getCellId() {
		return cellId;
	}

	public void setCellId(int cellId) {
		this.cellId = cellId;
	}

	public int getCallType() {
		return callType;
	}

	public void setCallType(int callType) {
		this.callType = callType;
	}

	public int getImsi() {
		return imsi;
	}

	public void setImsi(int imsi) {
		this.imsi = imsi;
	}

	public int getMsisdn() {
		return msisdn;
	}

	public void setMsisdn(int msisdn) {
		this.msisdn = msisdn;
	}

	public int getImei() {
		return imei;
	}

	public void setImei(int imei) {
		this.imei = imei;
	}

	public String getProtocolName() {
		return protocolName;
	}

	public void setProtocolName(String protocolName) {
		this.protocolName = protocolName;
	}

}
