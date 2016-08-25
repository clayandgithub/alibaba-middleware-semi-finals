/**
 * OrderLinePublisher.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

import com.lmax.disruptor.RingBuffer;

/**
 * @author wangweiwei
 *
 */
public class OrderLinePublisher extends Thread {

    private CountDownLatch latch;
    
    private StringEventPublisher producer;
    
    private String orderFile;
    
    private int fileNo;
    

    public OrderLinePublisher(RingBuffer<StringEvent> ringbuffer, CountDownLatch latch, String orderFile, int fileNo) {
        this.producer = new StringEventPublisher(ringbuffer);
        this.latch = latch;
        this.orderFile = orderFile;
        this.fileNo = fileNo;
    }

    public void run(){
        try {
            BufferedReader order_br = new BufferedReader(new InputStreamReader(new FileInputStream(orderFile)));
            long position = 0;
            String line = null;

            while ((line = order_br.readLine()) != null) {
                processLine(line, position);
                position += (line.getBytes().length + 1);
            }
            order_br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("OrderLinePublisher [" + orderFile + "] end~");
        latch.countDown();
    }

    /**
     * @param line
     */
    private void processLine(String line, long position) {
        producer.publish(line, fileNo, position);
    }

    /**
     * @param line
     */
//    private void processLine2(String line, long position) {
//        Long orderId = null;
//        String goodId = null;
//        String buyerId = null;
//        String createtime = null;
//        StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
//        while (stringTokenizer.hasMoreElements()) {
//            StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
//            String key = keyValue.nextToken();
//            String value = keyValue.nextToken();
//            if ("orderid".equals(key)) {
//                orderId = Long.parseLong(value);
//            } else if ("goodid".equals(key)) {
//                goodId = value;
//            } else if ("buyerid".equals(key)) {
//                buyerId = value;
//            } else if ("createtime".equals(key)) {
//                createtime = value;
//            }
//            if (orderId != null && goodId != null && buyerId != null && createtime != null) {
//                producer.publish(line, fileNo, position, orderId.longValue(), goodId, buyerId, createtime);
//                break;
//            }
//        }
//    }
}
