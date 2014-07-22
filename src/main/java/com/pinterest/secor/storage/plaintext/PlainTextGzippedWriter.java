package com.pinterest.secor.storage.plaintext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.storage.Writer;

public class PlainTextGzippedWriter implements Writer {

	private static final Logger LOG = LoggerFactory
			.getLogger(PlainTextGzippedWriter.class);

	private File mTmpFile;
	private FileOutputStream mFileOutputStream;
	private GZIPOutputStream mGzipOuputStream;
	private long mBytesWritten = 0;

	public PlainTextGzippedWriter(String destinationFile) throws IOException {
		mTmpFile = new File(destinationFile);

		if (!mTmpFile.exists()) {
			File parentFile = mTmpFile.getParentFile();
			if (!parentFile.exists() && !parentFile.mkdirs()) {
				throw new IOException(
						"Unable to create parent directory for path "
								+ destinationFile);
			}
		}

		mFileOutputStream = new FileOutputStream(mTmpFile);
		mGzipOuputStream = new GZIPOutputStream(mFileOutputStream);
	}

	@Override
	public void close() throws IOException {
		mGzipOuputStream.close();
		mGzipOuputStream.finish();
	}

	@Override
	public long getLength() throws IOException {
		return mBytesWritten;
	}

	@Override
	public void append(ParsedMessage message) throws IOException {

		LOG.debug(
				"Writing '{}' bytes with offset '{}' in Gzipped Plain Text format.",
				message.getPayload().length, message.getOffset());

		byte[] payload = message.getPayload();
		mGzipOuputStream.write(payload);
		mGzipOuputStream.write('\n');
		mBytesWritten += payload.length;
	}

	@Override
	public Object getImpl() {
		throw new NotImplementedException();
	}
}
