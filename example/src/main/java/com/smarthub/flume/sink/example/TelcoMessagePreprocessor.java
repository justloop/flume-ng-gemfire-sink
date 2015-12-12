/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package com.smarthub.flume.sink.example;

import java.util.Calendar;
import java.util.TimeZone;

import org.apache.flume.Context;
import org.apache.flume.Event;

import com.smarthub.flume.sink.MessagePreprocessor;
import com.smarthub.flume.sink.MessageTransformationException;
import com.smarthub.flume.sink.MessageWrapper;

/**
 * This is an example of a <code>MessagePreprocessor</code> implementation.
 */
public class TelcoMessagePreprocessor implements MessagePreprocessor {
	AifMessageWrapper aifObj = new AifMessageWrapper();
	GbMessageWrapper gbObj = new GbMessageWrapper();
	S1apMessageWrapper iucsObj = new S1apMessageWrapper();
	S1apMessageWrapper iupsObj = new S1apMessageWrapper();
	S1apMessageWrapper s1apObj = new S1apMessageWrapper();

	/**
	 * extract the hour of the time stamp as the key. So the data is partitioned
	 * per hour.
	 * 
	 * @param event
	 *            This is the Flume event that will be sent to Gemfire
	 * @param context
	 *            The Flume runtime context.
	 * @return Hour of the timestamp
	 */
	@Override
	public String extractKey(Event event, Context context) throws MessageTransformationException {
		// get timestamp header if it's present.
		String timestampStr = event.getHeaders().get("timestamp");
		if (timestampStr != null) {
			// parse it and get the hour
			Long timestamp = Long.parseLong(timestampStr);
			Calendar cal = Calendar.getInstance();
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));
			cal.setTimeInMillis(timestamp);
			return Integer.toString(cal.get(Calendar.HOUR_OF_DAY));
		}
		return null; // return null otherwise
	}

	/**
	 * Trying to prepend each message with the timestamp.
	 * 
	 * @param event
	 *            Flume event received by the sink.
	 * @param context
	 *            Flume context
	 * @return modified message of the form: timestamp + ":" + original message
	 *         body
	 */
	@Override
	public MessageWrapper transformMessage(Event event, Context context) throws MessageTransformationException {
		EventType eventType = EventType.valueOf(event.getHeaders().get("type"));
		switch (eventType) {
		case AIF: {
			aifObj.wrap(new String(event.getBody()));
			return aifObj;
		}
		case GB: {
			gbObj.wrap(new String(event.getBody()));
			return gbObj;
		}
		case IuCS: {
			iucsObj.wrap(new String(event.getBody()));
			return iucsObj;
		}
		case IuPS: {
			iupsObj.wrap(new String(event.getBody()));
			return iupsObj;
		}
		case S1AP: {
			s1apObj.wrap(new String(event.getBody()));
			return s1apObj;
		}
		default:
			throw new MessageTransformationException("Unable to transform message: " + event.getBody());
		}
	}
}
