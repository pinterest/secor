package com.pinterest.secor.io.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.protobuf.Message;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.OrcProtoUtil;
import com.pinterest.secor.util.OrcUtil;

public class OrcProtoReaderWriterFactory implements FileReaderWriterFactory{

	private static final Logger LOG = LoggerFactory.getLogger(OrcUtil.class);
	private OrcProtoUtil orcUtil;
	
	public OrcProtoReaderWriterFactory(SecorConfig config) {
		orcUtil = new OrcProtoUtil(config);
	}
	
	@Override
	public FileReader BuildFileReader(LogFilePath logFilePath,
			CompressionCodec codec) throws Exception {
		return new OrcFileReader(logFilePath);
	}

	@Override
	public FileWriter BuildFileWriter(LogFilePath logFilePath,
			CompressionCodec codec) throws Exception {
		return new OrcFileWriter(logFilePath);
	}
	
	protected class OrcFileReader implements FileReader {

		private Reader mReader;
		private VectorizedRowBatch rowBatch;
		private RecordReader recordReader;
		private long offset;
		private int rowIndex;
		private boolean next = false;
		private String topic;
		private Message.Builder messageBuilder;
		
		public OrcFileReader(LogFilePath path) {
			Configuration conf = new Configuration();
			Path filePath = new Path(path.getLogFilePath());
			offset = path.getOffset();
			try {
				mReader = OrcFile.createReader(filePath, OrcFile.readerOptions(conf));
				rowBatch = mReader.getSchema().createRowBatch();
				recordReader = mReader.rows();
				topic = path.getTopic();
				messageBuilder = orcUtil.getMessageInstance(topic).toBuilder();
			} catch (IOException e) {
				LOG.error("Error creating orc reader from filePath: {}, exception: {}", filePath, e);
				Throwables.propagate(e);
			}
		}
		
		@Override
		public KeyValue next() throws IOException {
			
			if (rowBatch.size == 0 || rowIndex >= rowBatch.size) {
				next = recordReader.nextBatch(rowBatch);
				rowIndex = 0;
			}
			if (!next)
				return null;
			Message result = orcUtil.convertFromOrcToProtoType(messageBuilder, rowBatch, rowIndex);
			rowIndex++;
			return new KeyValue(offset++, result.toByteArray());
		}

		@Override
		public void close() throws IOException {
			rowBatch = null;
			recordReader.close();
		}
		
	}
	
	protected class OrcFileWriter implements FileWriter {

		private Writer mWriter;
		private long rawDataSize;
		private VectorizedRowBatch rowBatch;
		private String topic;
		public OrcFileWriter(LogFilePath path) {
			Configuration conf = new Configuration();
			Path filePath = new Path(path.getLogFilePath());
			topic = path.getTopic();
			TypeDescription schema = orcUtil.getTopicOrcSchema(topic);
			try {
				mWriter = OrcFile.createWriter(filePath, OrcFile.writerOptions(conf).
						setSchema(schema));
			} catch (IOException e) {
				LOG.error("Error creating orc writer for the given schema: {}, Exception: {}", schema, e);
				Throwables.propagate(e);
			}
			rowBatch = schema.createRowBatch();
		}
		
		@Override
		public long getLength() throws IOException {
			return rawDataSize;
		}

		@Override
		public void write(KeyValue keyValue) throws IOException {
			Message protoMessage = orcUtil.decodeMessage(topic, keyValue.getValue());
			if (rowBatch.size == rowBatch.getMaxSize()) {
				mWriter.addRowBatch(rowBatch);
				rowBatch.reset();
			}
			orcUtil.convertFromProtoToOrcType(protoMessage, rowBatch, rowBatch.size);
			rawDataSize = rawDataSize + keyValue.getValue().length;
			rowBatch.size++;
		}

		@Override
		public void close() throws IOException {
			
			if (rowBatch.size != 0)
				mWriter.addRowBatch(rowBatch);
			rowBatch.reset();
			mWriter.close();
			rawDataSize = 0;
			
		}
	}
}
