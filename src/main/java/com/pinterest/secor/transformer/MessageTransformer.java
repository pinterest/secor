package com.pinterest.secor.transformer;

import com.pinterest.secor.message.Message;

/**
 * Message transformer And filter Interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface MessageTransformer {
    /**
     * Implement this method to add transformation logic at message level or filter out before
     * dumping it into Amazon S3
     * @return The transformed message or null if message should not be dumped to storage.
     */
    public Message transform(Message message);
}
