/**
 * CustomHandler.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wangweiwei
 *
 */
public class CustomHandler extends StringEventHandler {
    public static AtomicLong eventCount = new AtomicLong(0);
    public CustomHandler(int handlerId) {
        super(handlerId);
    }
    
    @Override
    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        long count = eventCount.addAndGet(1);
        System.out.println("[" + handlerId + "]" + "(" + count + ")" + " : " + event.content);
    }
}
