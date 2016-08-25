/**
 * StringEventHandler.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import com.lmax.disruptor.EventHandler;

/**
 * @author wangweiwei
 *
 */
public class StringEventHandler implements EventHandler<StringEvent> {

    int handlerId;

    public StringEventHandler(int handlerId) {
        this.handlerId = handlerId;
    }

    @Override
    public void onEvent(StringEvent event, long sequence, boolean endOfBatch)
            throws Exception {
        System.out.println("[" + handlerId + "]" + " : " + event.content);
    }
}
