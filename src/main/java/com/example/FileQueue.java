package com.example;

import com.amazonaws.services.sqs.model.Message;

/**
 * FileQueue
 * 
 * Interface used for FileQueue Storage Service with two implementations:
 * 
 * 1. FileQueuePrintWriter - New line delimited storage.
 * 
 * 2. FileQueueRAF - Position-based file storage. Should have approximately
 * equal push and pull times by removing the file re-write dependency. Requires
 * to run the clean up method eventually to reduce the filesize.
 * 
 * 
 * @author Edgar Resma
 */
public interface FileQueue {

	/*
	 * Add message to the end file queue.
	 */
	public void add(String queueUrl, String message);
	/*
	 * Returns a pulled message on top of the queue.
	 */
	public void reQueue(String queueUrl, String messageId, String message);
	/*
	 * Pull then delete the message from the top of the queue.
	 */
	public Message pull(String queueUrl);

}
