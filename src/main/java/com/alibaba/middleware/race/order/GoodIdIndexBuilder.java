package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.IndexSizeCache;
import com.alibaba.middleware.race.cache.TwoIndexCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.constant.IndexConstant;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * 1. 将所有 "未排序的按照goodid hash的order记录" 按照 "goodid以及orderid" 排序后存储 2.
 * 根据排序后的文件生成goodid一级索引文件 以及 二级索引缓存
 * 
 * 存放位置：第三个硬盘
 * 
 * @author jiangchao
 */
public class GoodIdIndexBuilder extends Thread {

    private CountDownLatch hashDownLatch;

    private CountDownLatch buildIndexCountLatch;

    private int maxConcurrentNum;

    public GoodIdIndexBuilder(CountDownLatch hashDownLatch,
            CountDownLatch buildIndexCountLatch, int maxConcurrentNum) {
        this.hashDownLatch = hashDownLatch;
        this.buildIndexCountLatch = buildIndexCountLatch;
        this.maxConcurrentNum = maxConcurrentNum;
    }

    public void build() {
        for (int i = 0; i < Config.ORDER_ONE_INDEX_FILE_NUMBER; i += maxConcurrentNum) {
            int concurrentNum = maxConcurrentNum > (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) ? (Config.ORDER_ONE_INDEX_FILE_NUMBER - i) : maxConcurrentNum;
            CountDownLatch multiIndexLatch = new CountDownLatch(concurrentNum);
            for (int j = i; j < i + concurrentNum; j++) {
                new GoodIdIndexBuilder.MultiIndex(j, multiIndexLatch, buildIndexCountLatch).start();
            }
            try {
                multiIndexLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run() {
        if (hashDownLatch != null) {
            try {
                hashDownLatch.await();// 等待上一个任务的完成
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long startTime = System.currentTimeMillis();
        build();
        System.out.printf("GoodIdIndexBuilder work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis() - OrderSystemImpl.constructStartTime);
    }

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
            Map<String, TreeMap<Long, String>> orderRankMap = new TreeMap<String, TreeMap<Long, String>>();
            Map<String, String> goodIndex = new LinkedHashMap<String, String>();
            TreeMap<String, Long> twoIndexMap = new TreeMap<String, Long>();
            try {
                BufferedReader orderBr = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream(Config.THIRD_DISK_PATH
                                                + FileConstant.UNSORTED_GOOD_ID_HASH_FILE_PREFIX
                                                + index)));

                BufferedWriter sortedHashBw = new BufferedWriter(
                        new FileWriter(Config.THIRD_DISK_PATH
                                + FileConstant.SORTED_GOOD_ID_HASH_FILE_PREFIX
                                + index));

                BufferedWriter sortedOneIndexBw = new BufferedWriter(
                        new FileWriter(Config.THIRD_DISK_PATH
                                        + FileConstant.SORTED_GOOD_ID_ONE_INDEX_FILE_PREFIX
                                        + index));
                //文件内容排序
                String rankStr = null;
                while ((rankStr = orderBr.readLine()) != null) {
                    String orderid = null;
                    String goodid = null;

                    StringTokenizer stringTokenizer = new StringTokenizer(rankStr, "\t");
                    while (stringTokenizer.hasMoreElements()) {

                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (IndexConstant.ORDER_ID.equals(key)) {
                            orderid = value;
                        } else if (IndexConstant.GOOD_ID.equals(key)) {
                            goodid = value;
                            if (!orderRankMap.containsKey(goodid)) {
                                orderRankMap.put(goodid, new TreeMap<Long, String>());
                            }
                        }
                        if (orderid != null && goodid != null) {
                            orderRankMap.get(goodid).put(Long.valueOf(orderid), rankStr);
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
                    Map<Long, String> val = (Map<Long, String>) entry.getValue();
                    int length = 0;
                    String goodid = key;

                    Iterator<Map.Entry<Long, String>> orderIdIterator = val.entrySet().iterator();
                    while (orderIdIterator.hasNext()) {
                        Map.Entry<Long, String> orderKv = orderIdIterator.next();
                        String orderKvValue = (String) orderKv.getValue();
                        sortedHashBw.write(orderKvValue + '\n');
                        length += orderKvValue.getBytes().length + 1;
                    }
                    if (!goodIndex.containsKey(goodid)) {
                        String posInfo = position + ":" + length + ":" + val.size();
                        goodIndex.put(goodid, posInfo);
                    }
                    position += length;
                    val.clear();
                }
                //将一级索引写入磁盘，同时生成二级索引放入内存
                int towIndexSize = (int) Math.sqrt(goodIndex.size());
                IndexSizeCache.goodIdIndexRegionSizeMap.put(index, towIndexSize);
                int count = 0;
                long oneIndexPosition = 0;
                Iterator<Map.Entry<String, String>> iterator = goodIndex.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, String> entry = iterator.next();
                    String key = (String) entry.getKey();
                    String val = (String) entry.getValue();
                    String content = key + ":" + val;
                    sortedOneIndexBw.write(content + '\n');

                    if (count % towIndexSize == 0) {
                        twoIndexMap.put(key, oneIndexPosition);
                    }
                    oneIndexPosition += content.getBytes().length + 1;
                    count++;
                }
                TwoIndexCache.goodIdTwoIndexCache.put(index, twoIndexMap);
                orderRankMap.clear();
                goodIndex.clear();
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
