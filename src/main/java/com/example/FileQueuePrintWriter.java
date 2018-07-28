package com.example;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.example.model.FileMessage;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

/**
 * FileMessageQueue
 * 
 * Implementation of a File Queue Storage Service. Messages are added
 * sequentially and are delimited by a new line. When adding or returning a new
 * message, a temp file is created containing messages from the old file with
 * the new message on top then replaces the old file upon completion.
 * 
 * @author Edgar Resma
 */
public class FileQueuePrintWriter implements FileQueue {

	private final static Logger LOGGER = Logger.getLogger(FileQueuePrintWriter.class.getName());
	
	private static File lock;
	private static File file;
	private static File temp;
	private static Properties prop = new Properties();

	/**
	 * Creates a new file or opens an existing file.
	 * 
	 * @param queueUrl
	 *            lock file directory to be created
	 */
	public void loadQueueFile(String queueUrl) {
		try {
			prop.load(getClass().getClassLoader().getResourceAsStream("aws.local.properties"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String storageLocation = prop.getProperty("filequeue.location");
		String queueName = fromUrl(queueUrl);
		try {
			file = new File(storageLocation + queueName + "\\messages");
			lock = new File(storageLocation + queueName + "\\.lock\\");
			temp = new File(storageLocation + queueName + "\\messages.temp");
			if (!file.exists()) {
				Files.createParentDirs(file);
				file.createNewFile();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Locks the file. Allows cross-process mutex lock by using mkdir.
	 * 
	 * @param lock
	 *            Lock file directory to be created.
	 */
	private void lock(File lock) throws InterruptedException {
		while (!lock.mkdirs()) {
			Thread.sleep(50);
		}
	}

	/**
	 * Unlocks the file to allow other processes to read or write.
	 * 
	 * @param lock
	 *            Lock file directory to be deleted.
	 */
	private void unlock(File lock) {
		lock.delete();
	}

	/**
	 * Add a message to the end of a Queue File
	 * 
	 * @param queueUrl
	 *            Filename of the file queue.
	 * @param message
	 *            message to be added into the queue
	 */
	public synchronized void add(String queueUrl, String message) {
		LOGGER.info("Adding to Queue: " + message);
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			String messageId = UUID.randomUUID().toString();
			FileMessage fm = new FileMessage(1, 0, messageId, message);
			PrintWriter writer = new PrintWriter(new FileOutputStream(file, true));
			// Writes the content to the file
			writer.println(fm);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} finally {
			unlock(lock);
		}
	}

	/**
	 * Re-queue a message onto the top Queue File
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 * @param message
	 *            Message to be added into the queue
	 */
	public synchronized void reQueue(String queueUrl, String messageId, String messageToReQueue) {
		LOGGER.severe("Requeueing: " + messageToReQueue);
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			BufferedReader br = new BufferedReader(new FileReader(file));
			List<String> messages = Files.readLines(file, Charsets.UTF_8);
			messageId = UUID.randomUUID().toString();
			FileMessage fm = new FileMessage(1, 0, messageId, messageToReQueue);
			PrintWriter writer = new PrintWriter(new FileOutputStream(temp, true));
			writer.println(fm);
			for (String message : messages) {
				writer.println(message);
			}
			writer.flush();
			writer.close();
			br.close();
			Files.move(temp, file);
			temp.delete();
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} finally {
			unlock(lock);
		}
	}

	/**
	 * Pull a message from the top of the Queue File and tag
	 * it as invisible.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 */
	public synchronized Message pull(String queueUrl) {
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			BufferedReader br = new BufferedReader(new FileReader(file));
			FileMessage fm = new FileMessage(br.readLine());
			List<String> messages = getMessages(br);
			PrintWriter writer = new PrintWriter(new FileOutputStream(temp, true));
			if (messages != null) {
				for (String message : messages) {
					writer.println(message);
				}
			}
			writer.flush();
			writer.close();
			br.close();
			Files.move(temp, file);
			LOGGER.info("Pulling from Queue" + fm);
			if (fm.getMessage() != null) {
				return new Message().withMessageId(fm.getReceiptId()).withBody(fm.getMessage());
			}
		} catch (EOFException e) {
			LOGGER.info("Queue is Empty");
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} finally {
			unlock(lock);
		}
		return null;
	}

	/**
	 * Get all succeeding messages starting from the current pointer
	 * location of the buffered reader.
	 * 
	 * @param br
	 * 		Buffered reader of the queue file.
	 * @return
	 * 		List of messages succeeding the pointer of the input.
	 */
	private synchronized List<String> getMessages(BufferedReader br) {
		String message;
		List<String> messages = new ArrayList<String>();
		try {
			while ((message = br.readLine()) != null) {
				messages.add(message);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return messages;
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
		return segments[segments.length - 1];
	}

}
