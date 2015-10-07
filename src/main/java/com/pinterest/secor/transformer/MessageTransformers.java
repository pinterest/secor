package com.pinterest.secor.transformer;

/**
 * Message transformer Interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface MessageTransformers {

    /**
     * Implement this method to add transformation logic at message level before
     * dumping it into Amazon S3
     */
    public byte[] transform(byte[] message);

}
