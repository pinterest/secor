package com.pinterest.secor.transformer;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

/**
 * Default message transformer class which does no transformation
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public class IdentityMessageTransformer implements MessageTransformer {
    protected SecorConfig mConfig;
    /**
     * Constructor
     * 
     * @param config
     */
    public IdentityMessageTransformer(SecorConfig config) {
        mConfig = config;
    }
    @Override
    public Message transform(Message message) {
        return message;
    }
}
