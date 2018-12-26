package com.pinterest.secor.message;

import java.util.Arrays;
import java.util.Objects;

public class MessageHeader {

	private final String key;
	private final byte[] value;

	public MessageHeader(String key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public byte[] getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		MessageHeader header = (MessageHeader) o;
		return (Objects.equals(key, header.key)) &&
				Arrays.equals(value, header.value);
	}

	@Override
	public int hashCode() {
		int result = key != null ? key.hashCode() : 0;
		result = 31 * result + Arrays.hashCode(value);
		return result;
	}
}
