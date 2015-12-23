package com.smarthub.flume.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.google.gson.Gson;

public class GemfireSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(GemfireSink.class);
	private Properties producerProps;
	private MessageWrapper messageWrapper;
	private MessagePreprocessor messagePreProcessor;
	private String region;
	private int batch_size;
	private Context context;
	private ClientCache cache;
	private Region<String, MessageWrapper> gfRegion;
	private int insertThreads;
	private Queue queue = new ConcurrentLinkedQueue();
	private ExecutorService executorService;

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		String eventKey = null;
		Map<String, MessageWrapper> eventMap = new HashMap<String, MessageWrapper>();
		try {
			transaction.begin();

			for (int i = 0; i < batch_size; i++) {

				event = channel.take();

				if (event != null) {
					// get the message body.
					String eventBody = new String(event.getBody());

					// log the event for debugging
					if (logger.isDebugEnabled()) {
						logger.debug(
								"{Event} header: " + new Gson().toJson(event.getHeaders()) + " body: " + eventBody);
					}

					// if the metadata extractor is set, extract the topic and
					// the
					// key.
					messageWrapper = null;
					if (messagePreProcessor != null) {
						messageWrapper = messagePreProcessor.transformMessage(event, context);
						eventKey = messagePreProcessor.extractKey(event, context);
					}
					eventMap.put(eventKey, messageWrapper);

				} else {
					// No event found, request back-off semantics from the sink
					// runner
					result = Status.BACKOFF;
				}
			}

			queue.add(eventMap);
			// publishing is successful. Commit.
			transaction.commit();

		} catch (Exception ex) {
			transaction.rollback();
			String errorMsg = "Failed to publish event: {Event} header: " + new Gson().toJson(event.getHeaders())
					+ " body: " + new String(event.getBody());
			logger.error(errorMsg);
			throw new EventDeliveryException(errorMsg, ex);

		} finally {
			transaction.close();
		}

		return result;
	}

	@Override
	public synchronized void start() {
		// load from gemfire config
		// Create the cache which causes the cache-xml-file to be parsed
		ClientCacheFactory factory = new ClientCacheFactory().set("name", "client").set("cache-xml-file",
				"gemfire-config.xml");
		for (Entry<Object, Object> e : producerProps.entrySet()) {
			factory = factory.set(e.getKey().toString(), e.getValue().toString());
		}
		cache = factory.create();

		// Get the Region
		gfRegion = cache.getRegion(region);
		super.start();

		logger.info("init thread pool executor...");
		executorService = Executors.newFixedThreadPool(insertThreads);
		for (int i = 0; i < insertThreads; i++) {
			executorService.submit(new GemFireWriterThread(gfRegion, queue));
		}
	}

	@Override
	public synchronized void stop() {
		cache.close();
		super.stop();
	}

	@Override
	public void configure(Context context) {
		this.context = context;
		// read the properties for Gemfire Producer
		// any property that has the prefix "gemfire" in the key will be
		// considered as a property that is passed when
		// instantiating the producer.
		// For example, gemfire.metadata.broker.list = localhost:9092 is a
		// property that is processed here, but not
		// sinks.k1.type = com.smarthub.flume.sink.GemfireSink.
		Map<String, String> params = context.getParameters();
		producerProps = new Properties();
		for (String key : params.keySet()) {
			String value = params.get(key).trim();
			key = key.trim();
			if (key.startsWith(Constants.PROPERTY_PREFIX)) {
				// remove the prefix
				key = key.substring(Constants.PROPERTY_PREFIX.length() + 1, key.length());
				producerProps.put(key.trim(), value);
				if (logger.isDebugEnabled()) {
					logger.debug("Reading a gemfire Producer Property: key: " + key + ", value: " + value);
				}
			}
		}

		// get the message Preprocessor if set
		String preprocessorClassName = context.getString(Constants.PREPROCESSOR);
		// if it's set create an instance using Java Reflection.
		if (preprocessorClassName != null) {
			try {
				Class preprocessorClazz = Class.forName(preprocessorClassName.trim());
				Object preprocessorObj = preprocessorClazz.newInstance();
				if (preprocessorObj instanceof MessagePreprocessor) {
					messagePreProcessor = (MessagePreprocessor) preprocessorObj;
				} else {
					String errorMsg = "Provided class for MessagePreprocessor does not implement "
							+ "'com.smarthub.flume.sink.MessagePreprocessor'";
					logger.error(errorMsg);
					throw new IllegalArgumentException(errorMsg);
				}
				logger.info("Preprocessor class name: " + preprocessorClassName);
			} catch (ClassNotFoundException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (InstantiationException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (IllegalAccessException e) {
				String errorMsg = "Error instantiating the MessagePreprocessor implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			}
		}

		// get the message Wrapper if set
		String messageWrapperClassName = context.getString(Constants.MESSAGE_WRAPPER);
		// if it's set create an instance using Java Reflection.
		if (messageWrapperClassName != null) {
			try {
				Class messageWrapperClazz = Class.forName(messageWrapperClassName.trim());
				Object messageWrapperObj = messageWrapperClazz.newInstance();
				if (messageWrapperObj instanceof MessageWrapper) {
					messageWrapper = (MessageWrapper) messageWrapperObj;
				} else {
					String errorMsg = "Provided class for MessageWrapper does not implement "
							+ "'com.smarthub.flume.sink.MessageWrapper'";
					logger.error(errorMsg);
					throw new IllegalArgumentException(errorMsg);
				}
				logger.info("Wrapper class name: " + messageWrapperClassName);
			} catch (ClassNotFoundException e) {
				String errorMsg = "Error instantiating the MessageWrapper implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (InstantiationException e) {
				String errorMsg = "Error instantiating the MessageWrapper implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			} catch (IllegalAccessException e) {
				String errorMsg = "Error instantiating the MessageWrapper implementation.";
				logger.error(errorMsg, e);
				throw new IllegalArgumentException(errorMsg, e);
			}
		}
		region = context.getString(Constants.REGION, Constants.DEFAULT_REGION);
		if (region.equals(Constants.DEFAULT_REGION)) {
			logger.warn("The Properties 'region' is not set. Using the default region name" + Constants.DEFAULT_REGION);
		} else {
			logger.info("Using the static region: " + region);
		}

		batch_size = context.getInteger(Constants.BATCH_SIZE, Constants.DEFAULT_BATCH_SIZE);
		if (batch_size == Constants.DEFAULT_BATCH_SIZE) {
			logger.warn("The Properties 'batch_size' is not set. Using the default batch size"
					+ Constants.DEFAULT_BATCH_SIZE);
		} else {
			logger.info("Using batch size: " + batch_size);
		}

		insertThreads = context.getInteger(Constants.INSERT_THREADS, Constants.DEFAULT_INSERT_THREADS);
		if (insertThreads == Constants.DEFAULT_INSERT_THREADS) {
			logger.warn("The Properties 'insert_threads' is not set. Using the default insert threads size"
					+ Constants.DEFAULT_INSERT_THREADS);
		} else {
			logger.info("Using insert threads: " + insertThreads);
		}
	}

	private class GemFireWriterThread implements Callable {
		Logger logger = LoggerFactory.getLogger(GemFireWriterThread.class);

		private Region region;
		private Queue queue;

		public GemFireWriterThread(Region<String, MessageWrapper> region, Queue queue) {
			this.region = region;
			this.queue = queue;
		}

		@Override
		public Object call() throws Exception {
			Object poll = null;
			int retryCounter = 0;
			while (retryCounter < Integer.MAX_VALUE) {
				poll = queue.poll();
				if (poll != null) {
					processData((Map) poll);
					retryCounter = 0;
				} else {
					Thread.currentThread().sleep(1000);
					retryCounter++;
				}
			}
			return 1;
		}

		private void processData(Map poll) {
			Map data = poll;
			region.putAll(data);
			if (logger.isDebugEnabled()) {
				logger.debug(Thread.currentThread().getId() + " --- Completed data put");
			}
		}
	}
}
