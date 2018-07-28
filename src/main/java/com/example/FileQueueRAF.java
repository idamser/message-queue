package com.example;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Properties;
import java.util.logging.Logger;

import com.amazonaws.services.sqs.model.Message;
import com.google.common.io.Files;

/**
 * FileQueue
 * 
 * My implementation of a queue file storage service for use in local
 * development. A local file is used to store the queue.
 * 
 * Queue file is composed of a 16 Byte Header and succeeded by Message Entries
 * of variable length.
 * 
 * Header Block 16 Bytes: <Head Position 8 Bytes><Tail Position 8 Bytes> - Used
 * to save the position of the pointer for the head and the tail
 * 
 * Entry Block: <Message Size 8 Bytes><Visible Flag 1 Byte><Message n Bytes> -
 * Message Size - contains the number of bytes in a message - Deleted Flag -
 * denotes if the entry is active in the queue or is inactive and ready for
 * removal from the queue - Message - Message of arbitrary size
 * 
 * Entries are added sequentially to the end and can be flagged as visible or
 * invisible in the queue. Traversal of entries is done by sequentially moving
 * from block to block using each Entry Block's computed size. Visible entries
 * are retrieved by the pull method then tagged as invisible. Entries are not
 * automatically deleted from the queue and are only flagged by the delete
 * method to improve performance during reading and writing without the need to
 * rewrite the whole document.
 * 
 * The file will grow as more messages are added to the queue. A clean-up method
 * should be invoked to reduce the file size. Clean-up searches for the first
 * visible entry starting from the first entry block position and creates a new file
 * starting with this visible entry. All invisible entries before are removed in
 * the process.
 * 
 * @author Edgar Resma
 */
public class FileQueueRAF implements FileQueue {

	private final static Logger LOGGER = Logger.getLogger(FileQueueRAF.class.getName());

	private final long HEADER_POSITION = 0;
	private final long HEADER_HEAD_POSITION = 0;
	private final long HEADER_TAIL_POSITION = 8;
	private final long ENTRY_HEADER_SIZE = 9;
	private final long VISIBLE_FLAG_POSITION = 8;
	private final long BODY_POSITION = 16;
	private final boolean FLAG_VISIBLE = true;
	private final boolean FLAG_INVISIBLE = false;
	private File file;
	private File lock;
	private File temp;
	private static RandomAccessFile raf;
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
				raf = new RandomAccessFile(file, "rw");
				initializeQueueFile(raf);
			}
			raf = new RandomAccessFile(file, "rw");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Locks the file. Allows cross-process mutex lock by using mkdir.
	 * 
	 * @param lock
	 *            lock file directory to be created
	 */
	private void lock(File lock) throws InterruptedException {
		while (!lock.mkdir()) {
			Thread.sleep(50);
		}
	}

	/**
	 * Unlocks the file to allow other process to read or write.
	 * 
	 * @param lock
	 *            lock file directory to be deleted
	 */
	private void unlock(File lock) {
		lock.delete();
	}

	/**
	 * This method initializes a random access file as a new QueueFile. Contents
	 * are deleted and headers are initialized to the first entry position.
	 * 
	 * @param raf
	 *            RandomAccessFile to be initialized as a Queue File with headers.
	 */
	private synchronized void initializeQueueFile(RandomAccessFile raf) {
		try {
			raf.getChannel().truncate(0);
			raf.seek(0);
			raf.writeLong(BODY_POSITION);
			raf.writeLong(BODY_POSITION);
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to add a message into the Queue File
	 * 
	 * @param queueUrl
	 *            Filename of the file queue.
	 * @param message
	 *            message to be added into the queue
	 */
	public synchronized void add(String queueUrl, String message) {
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			long fileEndPosition = raf.length();
			// go to end of file to append entry block
			raf.seek(fileEndPosition);
			// Write Entry
			raf.writeLong(message.length());
			raf.writeBoolean(FLAG_VISIBLE);
			raf.writeBytes(message);
			// Update queuefile header tail position
			raf.seek(HEADER_TAIL_POSITION);
			raf.writeLong(fileEndPosition);
			LOGGER.info("Adding to Queue: " + message + " new tail at " + fileEndPosition);
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} catch (InterruptedException e) {
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
	public synchronized void reQueue(String queueUrl, String messageId, String message) {
		long headPosition = Long.valueOf(messageId);
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			LOGGER.info("Requeue Started for:" + headPosition);
			raf.seek(headPosition);
			// read the message
			long length = raf.readLong();
			raf.seek(headPosition + ENTRY_HEADER_SIZE);
			byte[] b = new byte[(int) length];
			raf.readFully(b);
			// compare if it is the same message
			// block might have been cleaned-up so it will not match
			String fileMessage = new String(b);
			if (fileMessage.equals(message)) {
				// update to visible/re-queue the entry
				raf.seek(headPosition + VISIBLE_FLAG_POSITION);
				raf.writeBoolean(FLAG_VISIBLE);
				raf.seek(HEADER_HEAD_POSITION);
				// update head position if current point is lower
				if (headPosition < raf.readLong()) {
					raf.seek(HEADER_HEAD_POSITION);
					raf.writeLong(headPosition);
				}
				LOGGER.info("Requed: " + new String(b) + " new head position " + headPosition);
			}
		} catch (EOFException e) {
			LOGGER.info("Unable to requeue entry at: " + headPosition + " file already Cleaned up");
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
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
			// read QueueFile head entry position
			raf.seek(HEADER_HEAD_POSITION);
			long headPosition = raf.readLong();
			boolean visible = false;
			// while loop for the pointer to sequentially seek the next visible
			// entry in case head position is wrong
			while (visible == false) {
				raf.seek(headPosition);
				// read entry header
				long length = raf.readLong();
				// save entry position for re-queue
				long entryPosition = headPosition;
				visible = raf.readBoolean();
				if (visible == true) {
					// update to invisible
					raf.seek(headPosition + VISIBLE_FLAG_POSITION);
					raf.writeBoolean(FLAG_INVISIBLE);
					// move to message position and read the message
					raf.seek(headPosition + ENTRY_HEADER_SIZE);
					byte[] b = new byte[(int) length];
					raf.readFully(b);
					// update new head position to the next entry
					headPosition += length + ENTRY_HEADER_SIZE;
					raf.seek(HEADER_HEAD_POSITION);
					raf.writeLong(headPosition);
					LOGGER.info("Pulling from Queue: " + new String(b) + " new head position " + headPosition);
					return new Message().withMessageId(Long.toString(entryPosition)).withBody(new String(b));
				}
				headPosition += length + ENTRY_HEADER_SIZE;
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
	 * The purpose of this method is to remove all the invisible entries at the
	 * beginning of the QueueFile to save disk space. It begins searching the
	 * first visible entry and calls the deleteFlaggedItems method to create a
	 * new file starting from this entry. Invisible flags prior to this visible
	 * entry are excluded from the new file and are essentially deleted.
	 * 
	 * @param queueUrl
	 *            URL of the queue
	 */
	public synchronized void cleanUp(String queueUrl) {
		long pointer = BODY_POSITION;
		boolean visible = false;
		try {
			loadQueueFile(queueUrl);
			lock(lock);
			LOGGER.info("Clean-up Started");
			while (visible == false) {
				raf.seek(pointer);
				long length = raf.readLong();
				raf.seek(pointer + VISIBLE_FLAG_POSITION);
				visible = raf.readBoolean();
				if (visible == true) {
					raf.seek(HEADER_HEAD_POSITION);
					raf.writeLong(pointer);
					deleteFlaggedItems(queueUrl, pointer);
				}
				pointer += length + ENTRY_HEADER_SIZE;
			}
			LOGGER.info("Clean-up Completed");
		} catch (EOFException e) {
			LOGGER.info("Re-initializing Queue");
			initializeQueueFile(raf);
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
	 * This method is used to delete/truncate the contents of the
	 * RandomAccessFile prior to the input position. It creates a temp file.
	 * Starting from the input position, it copies all the contents until the
	 * end of the main file. This temp will replace the contents of the main
	 * file and then be deleted.
	 * 
	 * @param newHeadPosition
	 *            Starting point of the Queue File to truncate to
	 */
	private synchronized void deleteFlaggedItems(String queueUrl, long newHeadPosition) throws IOException {
		loadQueueFile(queueUrl);
		RandomAccessFile tempRaf = new RandomAccessFile(temp, "rw");
		initializeQueueFile(tempRaf);
		FileChannel raffc = raf.getChannel();
		FileChannel tempfc = tempRaf.getChannel();
		FileLock tempfl = tempfc.lock();
		long headPosition = newHeadPosition;
		try {
			LOGGER.info("Started removing deleted Messages");
			while (true) {
				raf.seek(headPosition);
				long length = raf.readLong();
				boolean flag = raf.readBoolean();
				byte[] b = new byte[(int) length];
				raf.readFully(b);
				tempRaf.writeLong(length);
				tempRaf.writeBoolean(flag);
				tempRaf.write(b);
				headPosition = headPosition + length + ENTRY_HEADER_SIZE;
			}
		} catch (EOFException e) {
			LOGGER.info("End of File Reached");
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			tempfc.transferTo(HEADER_POSITION, tempfc.size(), raffc.truncate(0));
		} catch (IOException e) {
			LOGGER.severe(e.getMessage());
			e.printStackTrace();
		} finally {
			tempfl.release();
			tempRaf.close();
			file.delete();
		}
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
