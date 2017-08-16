package com.unified.secor.io.impl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.util.ReflectionUtil;
import junit.framework.TestCase;

/**
 * Test the file readers and writers
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, DelimitedJsonToCsvFileReaderWriterFactory.class, GzipCodec.class,
        FileInputStream.class, FileOutputStream.class})
public class FileReaderWriterFactoryTest extends TestCase {

    private static final String DIR = "/some_parent_dir/some_topic/some_partition/some_other_partition";
    private static final String BASENAME = "10_0_00000000000000000100";
    private static final String PATH = DIR + "/" + BASENAME;
    private static final String PATH_GZ = DIR + "/" + BASENAME + ".gz";

    private LogFilePath mLogFilePath;
    private LogFilePath mLogFilePathGz;
    private SecorConfig mConfig;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mLogFilePath = new LogFilePath("/some_parent_dir", PATH);
        mLogFilePathGz = new LogFilePath("/some_parent_dir", PATH_GZ);
        System.setProperty("jolt.spec.override", "src/test/JoltSpecifications");
    }

    private void setupDelimitedTextFileWriterConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.unified.secor.io.impl.DelimitedJsonToCsvFileReaderWriterFactory");
        mConfig = new SecorConfig(properties);
    }

    private void mockDelimitedTextFileWriter(boolean isCompressed) throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(
                FileSystem.get(Mockito.any(URI.class),
                        Mockito.any(Configuration.class))).thenReturn(fs);

        Path fsPath = (!isCompressed) ? new Path(PATH) : new Path(PATH_GZ);

        GzipCodec codec = PowerMockito.mock(GzipCodec.class);
        PowerMockito.whenNew(GzipCodec.class).withNoArguments()
                .thenReturn(codec);

        FSDataInputStream fileInputStream = Mockito
                .mock(FSDataInputStream.class);
        FSDataOutputStream fileOutputStream = Mockito
                .mock(FSDataOutputStream.class);

        Mockito.when(fs.open(fsPath)).thenReturn(fileInputStream);
        Mockito.when(fs.create(fsPath)).thenReturn(fileOutputStream);

        CompressionInputStream inputStream = Mockito
                .mock(CompressionInputStream.class);
        CompressionOutputStream outputStream = Mockito
                .mock(CompressionOutputStream.class);
        Mockito.when(codec.createInputStream(Mockito.any(InputStream.class)))
                .thenReturn(inputStream);

        Mockito.when(codec.createOutputStream(Mockito.any(OutputStream.class)))
                .thenReturn(outputStream);
    }

    public void testDelimitedTextFileWriter() throws Exception {
        setupDelimitedTextFileWriterConfig();
        mockDelimitedTextFileWriter(false);
        FileWriter writer = (FileWriter) ReflectionUtil
                    .createFileWriter(mConfig.getFileReaderWriterFactory(),
                        mLogFilePath, null, mConfig
                );
        assert writer.getLength() == 0L;

        mockDelimitedTextFileWriter(true);
        writer = (FileWriter) ReflectionUtil
                .createFileWriter(mConfig.getFileReaderWriterFactory(),
                        mLogFilePathGz, new GzipCodec(), mConfig
                );
        assert writer.getLength() == 0L;
    }

    public void testDelimitedTextFileReader() throws Exception {
        setupDelimitedTextFileWriterConfig();

        mockDelimitedTextFileWriter(false);

        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePath, null, mConfig);

        mockDelimitedTextFileWriter(true);
        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, new GzipCodec(),
                mConfig);
    }
}