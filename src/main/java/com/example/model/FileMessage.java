package com.example.model;

public class FileMessage {
	/**
	 * FileMessage
	 * 
	 * Used to systematically contain parts of a message in the queue. This
	 * should be able return these parts as one message.
	 * 
	 * @author Edgar Resma
	 */
	private long requeueCount;
	private long visibilityDeadline;
	private String receiptId;
	private String message;

	public FileMessage() {
	}

	public FileMessage(String message) {
		try {
			if (message != null) {
				String[] parts = message.split(":");
				setRequeueCount(Long.valueOf(parts[0]));
				setVisibilityDeadline(Long.valueOf(parts[1]));
				setReceiptId(parts[2]);
				setMessage(parts[3]);
			}
		} catch (NumberFormatException e) {
			setRequeueCount(new Long(0));
			setVisibilityDeadline(new Long(0));
			setReceiptId(new String());
			setMessage(new String());
		}
	}

	public FileMessage(long requeueCount, long visibilityDeadline, String receiptId, String message) {
		this.setRequeueCount(requeueCount);
		this.setVisibilityDeadline(visibilityDeadline);
		this.setReceiptId(receiptId);
		this.setMessage(message);
	}

	public Long getRequeueCount() {
		return requeueCount;
	}

	public void setRequeueCount(Long requeueCount) {
		this.requeueCount = requeueCount;
	}

	public long getVisibilityDeadline() {
		return visibilityDeadline;
	}

	public void setVisibilityDeadline(long visibilityDeadline) {
		this.visibilityDeadline = visibilityDeadline;
	}

	public String getReceiptId() {
		return receiptId;
	}

	public void setReceiptId(String receiptId) {
		this.receiptId = receiptId;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	@Override
	public String toString() {
		return getRequeueCount() + ":" + getVisibilityDeadline() + ":" + getReceiptId() + ":" + getMessage();
	}

}
