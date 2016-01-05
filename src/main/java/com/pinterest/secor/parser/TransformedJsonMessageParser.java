package com.pinterest.secor.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import com.thomsonreuters.data.transform.TransformerFactory;

/**
 * Extract additional fields that are embedded in the json string from Kafka.
 * 
 */
public class TransformedJsonMessageParser extends JsonMessageParser {
	private static final Logger LOG = LoggerFactory.getLogger(TransformedJsonMessageParser.class);
	
	public TransformedJsonMessageParser(SecorConfig config) {
		super(config);
	}

	@Override
	public ParsedMessage parse(Message message) throws Exception {
		String[] partitions = extractPartitions(message);

		byte[] transformed = null;
		try {
			transformed = TransformerFactory.getTransformer(message.getTopic())
					.transform(new String(message.getPayload())).getBytes();
		} catch (Throwable t) {
			transformed = message.getPayload();
			//TODO, register the error in the monitor
			LOG.error("transform failed for message: " + new String(message.getPayload()), t);
		}
		return new ParsedMessage(message.getTopic(),
				message.getKafkaPartition(), message.getOffset(), message.getKafkaKey(), transformed,
				partitions);
	}
}
