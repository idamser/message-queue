package com.example;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.sqs.model.Message;

/**
 * In-Memory Queue Service
 * 
 * Memory-based implementation of Queue Service making use of
 * InMemoryQueue.
 * 
 * @author Edgar Resma
 */
public class InMemoryQueueService implements QueueService  {
	
	private InMemoryQueue queue = InMemoryQueue.getInstance();
	private ConcurrentMap<String, ScheduledFuture> invisibleQueue = new ConcurrentHashMap<String, ScheduledFuture>();
	private ScheduledExecutorService executorService;
	
	/**
	 * Constructor
	 * 
	 * @param executorService
	 *            executorService to be used by the queue
	 */
	public InMemoryQueueService(ScheduledExecutorService executorService) {
		this.executorService = executorService;
	}

	/**
	 * This method adds a message to the end of the Queue.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 */
	@Override
	public void push(String queueUrl, String message) {
		queue.add(queueUrl, message);
	}

	/**
	 * This method pulls a Message from top of the Queue.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @return Message An sqs message containing messageId to be used for the
	 *         delete method and the message from the queue
	 */
	@Override
	public Message pull(String queueUrl) {
		Message message = queue.pull(queueUrl);
		if (message != null) {
			makeInvisible(queueUrl, message.getMessageId(), message.getBody());
			return message;
		}
		return null;
	}

	/**
	 * Deletes a message by canceling the re-queuing of a message.
	 * 
	 * @param messageId
	 *            Identifier of the pulled message from the Queue File
	 */
	@Override
	public void delete(String queueUrl, String messageId) {
		ScheduledFuture<?> future = invisibleQueue.remove(messageId);
		if (future != null) {
			future.cancel(true);
		}
	}

	/**
	 * Schedules a task to return a message into the Queue if not deleted within
	 * a given timeout.
	 * 
	 * @param messageId
	 *            Identifier of the pulled message from the Queue File
	 * @param messageBody
	 *            Contents of the Message
	 */
	private void makeInvisible(String queueUrl, String messageId, String messageBody) {
		Runnable runnableTask = () -> {
			reQueue(queueUrl, messageId, messageBody);
		};
		ScheduledFuture<?> future = executorService.schedule(runnableTask, 2, TimeUnit.SECONDS);
		invisibleQueue.put(messageId, future);
	}

	/**
	 * Task that returns a message into the top of the queue
	 * 
	 * @param messageId
	 *            Identifier of the pulled message from the Queue File
	 * @param messageBody
	 *            Contents of the Message
	 */
	public void reQueue(String queueUrl, String messageId, String messageBody) {
		invisibleQueue.remove(messageId);
		queue.reQueue(queueUrl, messageId, messageBody);
	}

	/**
	 * Returns the number of messages pending for delete.
	 * 
	 */
	public int invisibleQueueSize() {
		return invisibleQueue.size();
	}
}
