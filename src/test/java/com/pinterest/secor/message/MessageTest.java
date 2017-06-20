package com.pinterest.secor.message;

import static org.junit.Assert.*;

import org.junit.Test;

public class MessageTest {

    @Test
    public void testNullPayload() {
	Message message = new Message("testTopic", 0, 123, null, null);
	System.out.println(message);

	// no assert necessary, just making sure it does not throw a
	// NullPointerException
    }

}
