
package com.unified.secor.io.impl;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DelimitedJsonToCsvFileReaderWriterFactoryTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGenerateTagToTransformerMap () {
        Map<String, List<String>> m = TagToColumns.loadTagToTransformerMap("src/test/joltSpecifications");
        Assert.assertEquals(1, m.size());
        Assert.assertNotNull(m.get("test"));

        List<String> columns = m.get("test");
        Assert.assertEquals("field1", columns.get(0));
        Assert.assertEquals("field2", columns.get(1));
    }

    @Test
    public void testGenerateTagToTransformerMap_unknownDirectory () {
        this.thrown.expect(RuntimeException.class);
        this.thrown.expectMessage("Invalid specification directory supplied.");
        TagToColumns.loadTagToTransformerMap("unknown");
    }
}
