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
 * 1.将所有 "未排序的按照buyerid hash的order记录" 按照 "buyerid以及createtime" 排序后存储 2.
 * 根据排序后的文件生成buyerid一级索引文件 以及 二级索引缓存
 * 
 * 存放位置：第二个硬盘
 * 
 * @author jiangchao
 */
public class BuyerIdIndexBuilder extends Thread {

    private CountDownLatch buyerIdOneIndexBuilderLatch;

    private CountDownLatch buildIndexCountLatch;

    private int maxConcurrentNum;

    public BuyerIdIndexBuilder(CountDownLatch buyerIdOneIndexBuilderLatch,
                               CountDownLatch buildIndexCountLatch, int maxConcurrentNum) {
        this.buyerIdOneIndexBuilderLatch = buyerIdOneIndexBuilderLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.maxConcurrentNum = maxConcurrentNum;
    }

    public void build() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i += maxConcurrentNum) {
            int concurrentNum = maxConcurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) : maxConcurrentNum;
            CountDownLatch multiIndexLatch = new CountDownLatch(concurrentNum);
            for (int j = i; j < i + concurrentNum; j++) {
                new BuyerIdIndexBuilder.MultiIndex(j, multiIndexLatch, buildIndexCountLatch).start();
            }
            try {
                multiIndexLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        if (buyerIdOneIndexBuilderLatch != null) {
            try {
                buyerIdOneIndexBuilderLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long startTime = System.currentTimeMillis();
        build();
        System.out.printf("BuyerIdTwoIndexBuilder work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis() - OrderSystemImpl.constructStartTime);
    }

    /**
     * 每个MultiIndex 负责一个一级索引文件，完成的任务有: 1. 将buyerid一级索引文件排序并存储 2.
     * 根据buyerid一级索引生成buyerid二级索引并缓存
     * 
     */
    private class MultiIndex extends Thread {
        private int index;
        private CountDownLatch selfCountDownLatch;
        private CountDownLatch parentCountDownLatch;

        public MultiIndex(int index, CountDownLatch selfCountDownLatch,
                CountDownLatch parentCountDownLatch) {
            this.index = index;
            this.selfCountDownLatch = selfCountDownLatch;
            this.parentCountDownLatch = parentCountDownLatch;
        }

        @Override
        public void run() {
            Map<String, TreeMap<Long, String>> orderRankMap = new TreeMap<String, TreeMap<Long, String>>();
            Map<String, String> buyerIndex = new LinkedHashMap<String, String>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            try {
                BufferedReader orderBr = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(
                                        Config.SECOND_DISK_PATH
                                                + FileConstant.UNSORTED_BUYER_ID_HASH_FILE_PREFIX
                                                + index)));

                BufferedWriter sortedHashBw = new BufferedWriter(
                        new FileWriter(Config.SECOND_DISK_PATH
                                       + FileConstant.SORTED_BUYER_ID_HASH_FILE_PREFIX
                                       + index));

                BufferedWriter sortedOneIndexBw = new BufferedWriter(
                        new FileWriter(
                                Config.SECOND_DISK_PATH
                                        + FileConstant.SORTED_BUYER_ID_ONE_INDEX_FILE_PREFIX
                                        + index));
                //文件内容排序
                String rankStr = null;
                while ((rankStr = orderBr.readLine()) != null) {
                    String buyerid = null;
                    String createtime = null;

                    StringTokenizer stringTokenizer = new StringTokenizer(rankStr, "\t");
                    while (stringTokenizer.hasMoreElements()) {

                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if ("createtime".equals(key)) {
                            createtime = value;
                        } else if ("buyerid".equals(key)) {
                            buyerid = value;
                            if (!orderRankMap.containsKey(buyerid)) {
                                orderRankMap.put(buyerid, new TreeMap<Long, String>());
                            }
                        }
                        if (createtime != null && buyerid != null) {
                            orderRankMap.get(buyerid).put(Long.valueOf(createtime), rankStr);
                            break;
                        }
                    }
                }
                //根据排序后的文件生成一级索引
                long position = 0;
                Iterator<Map.Entry<String, TreeMap<Long, String>>> orderRankIterator = orderRankMap.entrySet().iterator();
                while (orderRankIterator.hasNext()) {
                    Map.Entry<String, TreeMap<Long, String>> entry = orderRankIterator.next();
                    String key = (String) entry.getKey();
                    TreeMap<Long, String> val = (TreeMap<Long, String>) entry.getValue();
                    int length = 0;
                    String buyerid = key;
                    String posInfo = position + ":";
                    Iterator<Map.Entry<Long, String>> orderIdIterator = val.descendingMap().entrySet().iterator();
                    while (orderIdIterator.hasNext()) {
                        Map.Entry<Long, String> orderKv = orderIdIterator.next();
                        Long createTime = (Long) orderKv.getKey();
                        String orderKvValue = (String) orderKv.getValue();
                        sortedHashBw.write(orderKvValue + '\n');
                        posInfo = posInfo + createTime + "_" + (orderKvValue.getBytes().length + 1) + "|";
                        length += orderKvValue.getBytes().length + 1;

                    }
                    if (!buyerIndex.containsKey(buyerid)) {
                        buyerIndex.put(buyerid, posInfo);
                    }
                    position += length;
                    val.clear();
                }

                //将一级索引写入磁盘，同时生成二级索引放入内存
                int twoIndexSize = (int) Math.sqrt(buyerIndex.size());
                IndexSizeCache.buyerIdIndexRegionSizeMap.put(index, twoIndexSize);
                long count = 0;
                long oneIndexPosition = 0;
                Iterator<Map.Entry<String, String>> iterator = buyerIndex.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String key = (String) entry.getKey();
                    String val = (String) entry.getValue();
                    String content = key + ":" + val;
                    sortedOneIndexBw.write(content + '\n');

                    if (count % twoIndexSize == 0) {
                        twoIndexMap.put(key, oneIndexPosition);
                    }
                    oneIndexPosition += content.toString().getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.buyerIdTwoIndexCache.put(index, twoIndexMap);
                orderRankMap.clear();
                buyerIndex.clear();
                sortedOneIndexBw.flush();
                sortedOneIndexBw.close();
                sortedHashBw.flush();
                sortedHashBw.close();
                orderBr.close();
                selfCountDownLatch.countDown();
                parentCountDownLatch.countDown();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
