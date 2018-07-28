package com.example;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.amazonaws.services.sqs.model.Message;

public class FileQueueTest {

	private final String SQS_URL = "https://sqs.us-east-2.amazonaws.com/test/";
	ScheduledExecutorService scheduledExecutorService;
	FileQueueService queue;
	String queueUrl;
	File testFile;
	static Properties prop = new Properties();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		this.scheduledExecutorService = mock(ScheduledExecutorService.class);
		this.queue = new FileQueueService(scheduledExecutorService);
	}

	@After
	public void tearDown() {
		this.scheduledExecutorService.shutdown();
	}

	@Test
	public void When_AddingOneMessage_Expect_GetSameMessage() {
		// Arrange
		String queueUrl = SQS_URL + "When_AddingOneMessage_Expect_GetSameMessage";
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		String msg1 = "Message 1";
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		queue.push(queueUrl, msg1);
		// Assert
		assertEquals(msg1, queue.pull(queueUrl).getBody());
	}

	@Test
	public void When_AddingMessages_Expect_PollMessagesInSameOrderFIFO() {
		// Arrange
		String queueUrl = SQS_URL + "When_AddingMessages_Expect_PollMessagesInSameOrderFIFO";
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		String[] msgArray = { "Message 1", "Message 2", "Message 3", "Message 4" };
		String[] msgPullArray = new String[4];
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		for (int i = 0; i < msgArray.length; i++) {
			queue.push(queueUrl, msgArray[i]);
		}
		for (int i = 0; i < msgPullArray.length; i++) {
			msgPullArray[i] = queue.pull(queueUrl).getBody();
		}
		// Assert
		assertArrayEquals(msgArray, msgPullArray);
	}

	@Test
	public void When_PullingFromEmptyQueue_Expect_GetNull() {
		// Arrange
		String queueUrl = SQS_URL + "When_PullingFromEmptyQueue_Expect_GetNull";
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		// Assert
		assertNull(queue.pull(queueUrl));
	}

	@Test
	public void When_MessageIsPulledAndDeleted_Expect_NullMessagePulled() {
		// Arrange
		String queueUrl = SQS_URL + "When_MessageIsPulledAndDeleted_Expect_NullMessagePulled";
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		String msg1 = "Message 1";
		Message pull1 = null;
		Message pull2 = null;
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		queue.push(queueUrl, msg1);
		pull1 = queue.pull(queueUrl);
		queue.delete(queueUrl, pull1.getMessageId());
		pull2 = queue.pull(queueUrl);
		// Assert
		assertNull(pull2);
	}

	@Test
	public void When_MessageIsPulledAndNotDeleted_Expect_GetRequeuedMessagePulled() {
		// Arrange
		String queueUrl = SQS_URL + "When_MessageIsPulledAndNotDeleted_Expect_GetRequeuedMessagePulled";
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		String msg1 = "Message 1";
		Message pull1 = null;
		Message pull2 = null;
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		queue.push(queueUrl, msg1);
		pull1 = queue.pull(queueUrl);
		// manually run re-queue of the scheduled task
		queue.reQueue(queueUrl, pull1.getReceiptHandle(), pull1.getBody());
		pull2 = queue.pull(queueUrl);
		// Assert
		assertEquals(msg1, pull2.getBody());
	}

	@Test
	public void When_ProducerPushesAndAnotherConsumerPullsFromSameQueue_Expect_GetPushedMessageOnPull() {
		// Arrange
		String queueUrl = SQS_URL
				+ "When_ProducerPushesAndAnotherConsumerPullsFromSameQueue_Expect_GetPushedMessageOnPull";
		QueueService producer = new FileQueueService(scheduledExecutorService);
		QueueService consumer = new FileQueueService(scheduledExecutorService);
		ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
		String msg1 = "Message 1";
		Message pull1 = null;
		// Act
		doReturn(mockFuture).when(scheduledExecutorService).schedule(any(Runnable.class), anyLong(),
				any(TimeUnit.class));
		producer.push(queueUrl, msg1);
		pull1 = consumer.pull(queueUrl);
		// Assert
		assertEquals(msg1, pull1.getBody());
	}

	@Test
	public void When_PushAndPulledSimultaneously_Expect_NoErrorsAndAllMessagesQueuedAndPulled() {
		// Arrange
		String queueUrl = SQS_URL + "When_PushAndPulledSimultaneously_Expect_NoErrorsAndAllMessagesQueuedAndPulled";
		int executions = 100;
		int latchWait = 5;
		final ExecutorService executor = Executors.newFixedThreadPool(executions);
		final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(executions);
		QueueService queue = new FileQueueService(scheduledExecutor);
		String msg1 = "Message concurrent";
		List<String> recieipts = Collections.synchronizedList(new ArrayList());
		List<Runnable> runnables = new ArrayList<Runnable>();
		// Countdown latch used to wait until all threads are completed before
		// assert
		CountDownLatch latch = new CountDownLatch(executions);
		// Act
		for (int i = 0; i < executions; i++) {
			queue.push(queueUrl, msg1);
		}
		for (int i = 0; i < executions; i++) {
			runnables.add(() -> {
				Message message = queue.pull(queueUrl);
				recieipts.add(message.getReceiptHandle());
				queue.delete(queueUrl, message.getReceiptHandle());
				latch.countDown();
			});
		}
		for (Runnable runnable : runnables) {
			executor.execute(runnable);
		}
		try {
			latch.await(latchWait, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			executor.shutdown();
			scheduledExecutor.shutdown();
		}
		// Assert
		assertEquals(executions, recieipts.size());
	}
}
