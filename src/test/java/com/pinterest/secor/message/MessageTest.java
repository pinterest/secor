package com.pinterest.secor.message;

import org.junit.Test;

public class MessageTest {

    @Test
    public void testNullPayload() {
	Message message = new Message("testTopic", 0, 123, null, null, 0l);
	System.out.println(message);

	// no assert necessary, just making sure it does not throw a
	// NullPointerException
    }

    @Test
    public void testBinaryKeyAndPayloadToString() {
        Message message = new Message("testTopic", 0, 123,
                new byte[]{(byte)0x80}, new byte[]{(byte)0x80}, 0l);
        // no assert necessary, just making sure it does not throw a
        // NullPointerException
        System.out.println(message);
    }

}
