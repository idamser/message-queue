package com.example;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;

/**
 * In-Memory Queue
 * 
 * Implementation of memory-based Queue Messaging Service. A Blocking Dequeue
 * is used to allow re-queuing of messages not deleted after being pulled.
 * 
 * @author Edgar Resma
 */
public class InMemoryQueue {
	
	private final static Logger LOGGER = Logger.getLogger(InMemoryQueue.class.getName());
	
	private static InMemoryQueue inMemoryQueue;
	private BlockingDeque<String> messages;
	private ConcurrentMap<String, BlockingDeque<String>> queue = new ConcurrentHashMap<String, BlockingDeque<String>>();

	
	public static InMemoryQueue getInstance() {
		if (inMemoryQueue == null) {
			inMemoryQueue = new InMemoryQueue();
		}
		return inMemoryQueue;
	}
	
	
	private InMemoryQueue() {
	}


	/**
	 * This method adds a message to the end of the Queue.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 */
	public void add(String queueUrl, String message) {
		String queueName = fromUrl(queueUrl);
		LOGGER.info("Adding to Queue: " + message);
		if (queue.get(queueName) != null) {
			queue.get(queueName).add(message);
		} else {
			messages = new LinkedBlockingDeque<String>();
			messages.add(message);
			queue.put(queueName, messages);
		}
	}

	/**
	 * This method pulls a Message from top of the Queue.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @return Message An sqs message containing messageId to be used for the
	 *         delete method and the message from the queue
	 */
	public Message pull(String queueUrl) {
		String queueName = fromUrl(queueUrl);
		String messageBody;
		try {
			LOGGER.info("Pulling from queue");
			messageBody = queue.get(queueName).poll();
		} catch (NullPointerException e) {
			LOGGER.info("Queue Empty");
			return null;
		}
		String messageId = "";
		if (messageBody != null) {
			messageId = UUID.randomUUID().toString();
		}
		return new Message().withMessageId(messageId).withReceiptHandle(messageId).withBody(messageBody);

	}

	/**
	 * Task that returns a message into the top of the queue
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param messageId
	 *            Identifier of the pulled message from the Queue File
	 * @param messageBody
	 *            Contents of the Message
	 */
	public void reQueue(String queueUrl, String messageId, String messageBody) {
		String queueName = fromUrl(queueUrl);
		LOGGER.info("Requeue Started for:" + messageId);
		messages.addFirst(messageBody);
		if (queue.get(queueName) != null) {
			queue.get(queueName).addFirst(messageBody);
		} else {
			messages = new LinkedBlockingDeque<String>();
			messages.add(messageBody);
			queue.put(queueName, messages);
		}
		LOGGER.info("Requeue Completed for:" + messageId);
	}
	/**
	 * Get queue name from URL
	 * 
	 * @param queueUrl
	 *            URL used for sqs service
	 * @return Queue name
	 * 
	 */
	private String fromUrl(String queueUrl) {
		URI uri = URI.create(queueUrl);
		String[] segments = uri.getPath().split("/");		
		return segments[segments.length-1];
	}
}
