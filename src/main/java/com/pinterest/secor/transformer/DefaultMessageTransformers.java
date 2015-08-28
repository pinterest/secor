package com.pinterest.secor.transformer;

import com.pinterest.secor.common.SecorConfig;

public class DefaultMessageTransformers implements MessageTransformers {

	protected SecorConfig mConfig;

	public DefaultMessageTransformers(SecorConfig config) {
		mConfig = config;
	}

	@Override
	public byte[] transform(byte[] message) {
		return message;
	}

}
