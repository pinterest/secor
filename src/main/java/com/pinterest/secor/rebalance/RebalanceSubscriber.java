package com.pinterest.secor.rebalance;

import com.pinterest.secor.common.SecorConfig;

public interface RebalanceSubscriber {
    void subscribe(RebalanceHandler handler, SecorConfig config);
}
