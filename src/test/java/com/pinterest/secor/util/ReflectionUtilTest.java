package com.pinterest.secor.util;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReaderWriter;
import com.pinterest.secor.parser.MessageParser;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class ReflectionUtilTest {

    private SecorConfig mSecorConfig;
    private LogFilePath mLogFilePath;

    public void setUp() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        mSecorConfig = new SecorConfig(properties);
        mLogFilePath = new LogFilePath("/foo", "/foo/bar/baz/1_1_1");
    }

    @Test
    public void testCreateMessageParser() throws Exception {
        MessageParser messageParser = ReflectionUtil.createMessageParser("com.pinterest.secor.parser.OffsetMessageParser",
                mSecorConfig);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testMessageParserClassNotFound() throws Exception {
        ReflectionUtil.createMessageParser("com.example.foo", mSecorConfig);
    }
    @Test(expected = ClassNotFoundException.class)
    public void testFileReaderWriterClassNotFound() throws Exception {
        ReflectionUtil.createFileReaderWriter("com.example.foo", mLogFilePath, null, FileReaderWriter.Type.Writer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMessageParserConstructorMissing() throws Exception {
        ReflectionUtil.createMessageParser("org.apache.commons.configuration.PropertiesConfiguration", mSecorConfig);
    }
    @Test(expected = IllegalArgumentException.class)
    public void testFileReaderWriterConstructorMissing() throws Exception {
        ReflectionUtil.createFileReaderWriter("org.apache.commons.configuration.PropertiesConfiguration",
                mLogFilePath, null, FileReaderWriter.Type.Writer);
    }
}