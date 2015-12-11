package com.smarthub.flume.sink;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

public class GemfireSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(GemfireSink.class);
	private Properties producerProps;
	private MessageWrapper messageWrapper;
	private MessagePreprocessor messagePreProcessor;
	private String region;
	private Context context;

	private Cache cache;
	private Region<String, MessageWrapper> gfRegion;

	@Override
	public Status process() throws EventDeliveryException {
		Status result = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();
		Event event = null;
		String eventKey = null;

		try {
			transaction.begin();
			event = channel.take();

			if (event != null) {
				// get the message body.
				String eventBody = new String(event.getBody());

				// log the event for debugging
				if (logger.isDebugEnabled()) {
					logger.debug("{Event} " + eventBody);
				}

				// if the metadata extractor is set, extract the topic and the
				// key.
				messageWrapper = null;
				if (messagePreProcessor != null) {
					messageWrapper = messagePreProcessor.transformMessage(event, context);
					eventKey = messagePreProcessor.extractKey(event, context);
				}
				gfRegion.put(eventKey, messageWrapper);

			} else {
				// No event found, request back-off semantics from the sink
				// runner
				result = Status.BACKOFF;
			}
			// publishing is successful. Commit.
			transaction.commit();

		} catch (Exception ex) {
			transaction.rollback();
			String errorMsg = "Failed to publish event: " + event;
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
		CacheFactory factory = new CacheFactory().set("cache-xml-file", "gemfire-config.xml");
		for (Entry<Object, Object> e : producerProps.entrySet()) {
			factory.set(e.getKey().toString(), e.getValue().toString());
		}
		cache = factory.create();

		// Get the Region
		gfRegion = cache.getRegion(region);
		super.start();
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
		// sinks.k1.type = com.thilinamb.flume.sink.GemfireSink.
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

		if (messagePreProcessor == null) {
			// MessagePreprocessor is not set. So read the topic from the
			// config.
			region = context.getString(Constants.REGION, Constants.DEFAULT_REGION);
			if (region.equals(Constants.DEFAULT_REGION)) {
				logger.warn("The Properties 'metadata.extractor' or 'region' is not set. Using the default region name"
						+ Constants.DEFAULT_REGION);
			} else {
				logger.info("Using the static region: " + region);
			}
		}
	}
}
