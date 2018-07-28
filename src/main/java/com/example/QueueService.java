package com.example;

import com.amazonaws.services.sqs.model.Message;

public interface QueueService {

	/**
	 * This method adds a message into the end of the QueueFile.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 */
	void push(String queueUrl, String message);
	/**
	 * This method pulls the value from the top of Queue File and deletes it.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @return Message An sqs message containing messageId to be used for the
	 *         delete method and the message from the queue
	 */
	Message pull(String queueUrl);
	/**
	 * This method deletes the message using the given receipt handle..
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param receiptHandle
	 *            Identifier of the message to be deleted
	 */
	void delete(String queueUrl, String receiptHandle);
	
}
