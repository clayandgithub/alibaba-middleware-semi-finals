/**
 * TestDisruptor.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

/**
 * @author wangweiwei
 *
 */
public class TestDisruptor {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        EventFactory<StringEvent> eventFactory = new StringEventFactory();
        int ringBufferSize = 1024 * 1024; // RingBuffer 大小，必须是 2 的 N 次方；
//        ExecutorService executor = Executors.newSingleThreadExecutor();
        SimpleThreadFactory threadFactory = SimpleThreadFactory.INSTANCE;
        Disruptor<StringEvent> disruptor = new Disruptor<StringEvent>(eventFactory,
                        ringBufferSize, threadFactory, ProducerType.MULTI, new YieldingWaitStrategy());
        StringEventHandler[] handlers = new StringEventHandler[10];
        for (int i = 0; i < 10; ++i) {
            handlers[i] = new CustomHandler(i);
        }
        disruptor.handleEventsWith(handlers);
        disruptor.start();
        
        final CountDownLatch producerLatch = new CountDownLatch(3);
        CustomPublisher publisher1 = new CustomPublisher(disruptor.getRingBuffer(), producerLatch);
        CustomPublisher publisher2 = new CustomPublisher(disruptor.getRingBuffer(), producerLatch);
        CustomPublisher publisher3 = new CustomPublisher(disruptor.getRingBuffer(), producerLatch);
        publisher1.start();
        publisher2.start();
        publisher3.start();
        try {
            producerLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        disruptor.shutdown();
        System.out.println("Total used time : " + (System.currentTimeMillis() - startTime));
        System.out.println(CustomHandler.eventCount.get());
    }
}
