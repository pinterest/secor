/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.io;

/**
 * Generic Object used to read next message from various file reader
 * implementations
 *
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class KeyValue {

	private final long mOffset;
	private final byte[] mKafkaKey;
	private final byte[] mValue;
	private final long mTimestamp;

	// constructor
	public KeyValue(long offset, byte[] value) {
		this.mOffset = offset;
		this.mKafkaKey = new byte[0];
		this.mValue = value;
		this.mTimestamp = -1;
	}

	// constructor
	public KeyValue(long offset, byte[] kafkaKey, byte[] value) {
		this.mOffset = offset;
		this.mKafkaKey = kafkaKey;
		this.mValue = value;
		this.mTimestamp = -1;
	}

	// constructor
	public KeyValue(long offset, byte[] kafkaKey, byte[] value, long timestamp) {
		this.mOffset = offset;
		this.mKafkaKey = kafkaKey;
		this.mValue = value;
		this.mTimestamp = timestamp;
	}

	public long getOffset() {
		return this.mOffset;
	}

	public byte[] getKafkaKey() {
		return this.mKafkaKey;
	}

	public byte[] getValue() {
		return this.mValue;
	}

	public long getTimestamp() {
		return this.mTimestamp;
	}

	public boolean hasKafkaKey() {
		return this.mKafkaKey != null && this.mKafkaKey.length != 0;
	}

	public boolean hasTimestamp(){
		return this.mTimestamp != -1;
	}
}
