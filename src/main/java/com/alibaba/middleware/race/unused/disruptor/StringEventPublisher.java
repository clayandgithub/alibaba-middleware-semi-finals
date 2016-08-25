/**
 * StringEventPublisher.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import com.lmax.disruptor.RingBuffer;

/**
 * @author wangweiwei
 *
 */
public class StringEventPublisher {
    private RingBuffer<StringEvent> ringbuffer;

    public StringEventPublisher(RingBuffer<StringEvent> ringbuffer) {
        this.ringbuffer = ringbuffer;
    }

    public void publish(String data, int fileNo, long position) {
        long seq = ringbuffer.next();
        try {
            StringEvent evt = ringbuffer.get(seq);
            evt.content = data;
            evt.fileNo = fileNo;
            evt.position = position;
        } finally {
            ringbuffer.publish(seq);
        }
    }

}
