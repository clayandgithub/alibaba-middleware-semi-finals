package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 1. 将所有未排序的orderid一级索引文件排序并存储 2. 根据orderid一级索引生成orderid二级索引并缓存
 * 
 * 存放位置：第一个硬盘
 * 
 * @author jiangchao
 */
public class OrderIdTwoIndexBuilder extends Thread {

    private CountDownLatch orderIdOneIndexBuilderLatch;

    private CountDownLatch buildIndexCountLatch;

    private int maxConcurrentNum;

    public OrderIdTwoIndexBuilder(CountDownLatch orderIdOneIndexBuilderLatch, CountDownLatch buildIndexCountLatch, int maxConcurrentNum) {
        this.orderIdOneIndexBuilderLatch = orderIdOneIndexBuilderLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.maxConcurrentNum = maxConcurrentNum;
    }

    public void build() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i += maxConcurrentNum) {
            int concurrentNum = maxConcurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) : maxConcurrentNum;
            CountDownLatch multiIndexLatch = new CountDownLatch(concurrentNum);
            for (int j = i; j < i + concurrentNum; j++) {
                new MultiIndex(j, multiIndexLatch, buildIndexCountLatch).start();
            }
            try {
                multiIndexLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        if (orderIdOneIndexBuilderLatch != null) {
            try {
                orderIdOneIndexBuilderLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long startTime = System.currentTimeMillis();
        build();
        System.out.printf("OrderIdTwoIndexBuilder work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis() - OrderSystemImpl.constructStartTime);
    }

    /**
     * 每个MultiIndex 负责一个一级索引文件，完成的任务有: 1. 将orderid一级索引文件排序并存储 2.
     * 根据orderid一级索引生成orderid二级索引并缓存
     * 
     */
    private class MultiIndex extends Thread {
        private int index;
        private CountDownLatch selfCountDownLatch;
        private CountDownLatch parentCountDownLatch;

        public MultiIndex(int index, CountDownLatch selfCountDownLatch, CountDownLatch parentCountDownLatch) {
            this.index = index;
            this.selfCountDownLatch = selfCountDownLatch;
            this.parentCountDownLatch = parentCountDownLatch;
        }

        @Override
        public void run() {
            Map<Long, String> orderIndex = new TreeMap<Long, String>();
            TreeMap<Long, Long> twoIndexMap = new TreeMap<Long, Long>();
            try {
                BufferedReader oneIndexBr = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(
                                        Config.FIRST_DISK_PATH
                                                + FileConstant.UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                                                + index)));
                BufferedWriter sortedOneIndexBw = new BufferedWriter(
                        new FileWriter(
                                Config.FIRST_DISK_PATH
                                        + FileConstant.SORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                                        + index));
                String str = null;
                long count = 0;
                while ((str = oneIndexBr.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, ":");
                    while (stringTokenizer.hasMoreElements()) {
                        orderIndex.put(Long.valueOf(stringTokenizer.nextToken()), str);
                        break;
                    }
                }

                int towIndexSize = (int) Math.sqrt(orderIndex.size());
                IndexSizeCache.orderIdIndexRegionSizeMap.put(index, towIndexSize);
                count = 0;
                long position = 0;
                Iterator<Map.Entry<Long, String>> iterator = orderIndex.entrySet().iterator();
                while (iterator.hasNext()) {

                    Map.Entry<Long, String> entry = iterator.next();
                    Long key = (Long) entry.getKey();
                    String val = (String) entry.getValue();
                    sortedOneIndexBw.write(val + '\n');

                    if (count % towIndexSize == 0) {
                        twoIndexMap.put(key, position);
                    }
                    position += val.getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.orderIdTwoIndexCache.put(index, twoIndexMap);
                orderIndex.clear();
                sortedOneIndexBw.flush();
                sortedOneIndexBw.close();
                oneIndexBr.close();
                selfCountDownLatch.countDown();
                parentCountDownLatch.countDown();
            } catch (FileNotFoundException e1) {
                e1.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
