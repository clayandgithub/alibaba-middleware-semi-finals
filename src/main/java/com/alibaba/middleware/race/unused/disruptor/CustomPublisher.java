/**
 * CustomProducer.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.util.StringUtil;
import com.lmax.disruptor.RingBuffer;

/**
 * @author wangweiwei
 *
 */
public class CustomPublisher extends Thread {

    private CountDownLatch latch;
    
    private StringEventPublisher producer;

    public CustomPublisher(RingBuffer<StringEvent> ringbuffer, CountDownLatch latch) {
        this.producer = new StringEventPublisher(ringbuffer);
        this.latch = latch;
    }

    public void run(){
        for (int i = 0; i < 100; ++i) {
            String randomSting = StringUtil.genRandomString(512);
            producer.publish(randomSting, 0, 0);
        }

        System.out.println("CustomProducer end~");
        latch.countDown();
    }
}
