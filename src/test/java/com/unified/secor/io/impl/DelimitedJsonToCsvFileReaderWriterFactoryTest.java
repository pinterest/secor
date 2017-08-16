
package com.unified.secor.io.impl;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class DelimitedJsonToCsvFileReaderWriterFactoryTest {
    @Test
    public void testGenerateTagToTransformerMap () {
        Map<String, List<String>> m = TagToColumns.loadTagToTransformerMap("src/test/JoltSpecifications");
        Assert.assertEquals(1, m.size());
        Assert.assertNotNull(m.get("test"));
    }
}
