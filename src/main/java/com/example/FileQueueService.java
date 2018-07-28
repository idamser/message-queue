package com.example;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import com.amazonaws.services.sqs.model.Message;

/**
 * File Queue Service
 * 
 * File Queue implementation of Queue Service making use of
 * FileQueue as a file storage service.
 * 
 * @author Edgar Resma
 */
public class FileQueueService implements QueueService {

	private ConcurrentMap<String, ScheduledFuture> invisibleQueue = new ConcurrentHashMap<String, ScheduledFuture>();
	private ScheduledExecutorService executorService;
	private FileQueue queueFile;
	private long visibilityTimeout;
	static Properties prop;

	/**
	 * Constructor
	 * 
	 * @param executorService
	 *            executorService to be used by the queue
	 */
	public FileQueueService(ScheduledExecutorService executorService) {
		this.executorService = executorService;
		initializedFileStorageService();
	}
	/**
	 * Initialized the File Storage Service Used based from properties file:
	 * 1. FileQueuePrintWriter - will be used as default.
	 * 2. FileQueueRAF - position-based file storage. 
	 * 
	 * Properties file also contain the timeout default used for re-queue
	 * and storage location to be used on local machine.
	 * 
	 */
	private void initializedFileStorageService() {
		try {
			prop = new Properties();
			prop.load(getClass().getClassLoader().getResourceAsStream("aws.local.properties"));
			String fileService = prop.getProperty("filequeue.impl");
			visibilityTimeout = Long.valueOf(prop.getProperty("timeout.default"));
			switch (fileService) {
			case "FileQueueRAF":
				queueFile = new FileQueueRAF();
				break;

			default:
				queueFile = new FileQueuePrintWriter();
				break;
			} 
		} catch (IOException e) {
			queueFile = new FileQueuePrintWriter();
			e.printStackTrace();
		}
	
	}
	
	/**
	 * This method adds a message into the end of the QueueFile.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 */
	@Override
	public void push(String queueUrl, String message) {
		queueFile.add(queueUrl, message);
	}

	/**
	 * This method adds a message into the QueueFile for of multiple consumers.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 * @param count
	 *            Number of times to re-add the message
	 */
	public void push(String queueUrl, String message, long count) {
		for (long i = 0; i < count; i++) {
			queueFile.add(queueUrl, message);
		}
	}

	/**
	 * This method pulls the value from the top of Queue File and deletes it.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @return Message An sqs message containing messageId to be used for the
	 *         delete method and the message from the queue
	 */
	@Override
	public Message pull(String queueUrl) {
		Message messageBody = queueFile.pull(queueUrl);
		if (messageBody != null && !messageBody.getBody().isEmpty()) {
			makeInvisibleTimer(queueUrl, messageBody.getMessageId(), messageBody.getBody());
			return new Message().withMessageId(messageBody.getMessageId()).withReceiptHandle(messageBody.getMessageId())
					.withBody(messageBody.getBody());
		}
		return null;

	}

	/**
	 * Deletes a message by canceling the re-queuing of a message.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param receiptHandle
	 *            Identifier of the pulled message from the Queue File
	 */
	@Override
	public void delete(String queueUrl, String receiptHandle) {
		ScheduledFuture<?> future = invisibleQueue.remove(receiptHandle);
		if (future != null) {
			future.cancel(true);
		}
	}

	/**
	 * Schedules a task to return a message into the QueueFile if not deleted
	 * within a given timeout.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param messageId
	 *            Identifier of the pulled message from the Queue File
	 * @param messageBody
	 *            Contents of the Message
	 */
	private void makeInvisibleTimer(String queueUrl, String messageId, String messageBody) {
		Runnable runnableTask = () -> {
			reQueue(queueUrl, messageId, messageBody);
		};
		ScheduledFuture<?> future = executorService.schedule(runnableTask, visibilityTimeout, TimeUnit.SECONDS);
		invisibleQueue.put(messageId, future);
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
		invisibleQueue.remove(messageId);
		queueFile.reQueue(queueUrl, messageId, messageBody);
	}

	/**
	 * @returns Number of messages waiting to be deleted.
	 */
	public int invisibleQueueSize() {
		return invisibleQueue.size();
	}

}
