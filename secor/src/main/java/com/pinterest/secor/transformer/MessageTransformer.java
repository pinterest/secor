package com.pinterest.secor.transformer;

import com.pinterest.secor.message.Message;

/**
 * Message transformer Interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface MessageTransformer {
    /**
     * Implement this method to add transformation logic at message level before
     * dumping it into Amazon S3
     */
    public Message transform(Message message);
}
