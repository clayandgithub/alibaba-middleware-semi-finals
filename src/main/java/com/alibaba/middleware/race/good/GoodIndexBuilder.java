package com.alibaba.middleware.race.good;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.cache.KeyCache;
import com.alibaba.middleware.race.cache.OneIndexCache;
import com.alibaba.middleware.race.constant.IndexConstant;
import com.alibaba.middleware.race.model.FilePosition;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * 根据goodid生成good的索引并缓存 Created by jiangchao on 2016/7/13.
 */
public class GoodIndexBuilder extends Thread {

    private CountDownLatch waitCountDownLatch;
    private Collection<String> goodFiles;
    private CountDownLatch countDownLatch;
    private int fileBeginNo;

    public GoodIndexBuilder(CountDownLatch waitCountDownLatch,
            Collection<String> goodFiles, CountDownLatch countDownLatch,
            int fileBeginNo) {
        this.waitCountDownLatch = waitCountDownLatch;
        this.goodFiles = goodFiles;
        this.countDownLatch = countDownLatch;
        this.fileBeginNo = fileBeginNo;
    }

    public void hash() {

        int count = 0;
        for (String goodFile : goodFiles) {
            FileNameCache.fileNameMap.put(fileBeginNo, goodFile);
            BufferedReader good_br = null;
            try {
                FileInputStream good_records = new FileInputStream(goodFile);
                good_br = new BufferedReader(new InputStreamReader(good_records));

                String str = null;
                long position = 0;
                while ((str = good_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (!KeyCache.goodKeyCache.containsKey(key)) {
                            KeyCache.goodKeyCache.put(key, 0);
                        }
                        if (IndexConstant.GOOD_ID.equals(key)) {
                            FilePosition filePosition = new FilePosition(fileBeginNo, position);
                            OneIndexCache.goodOneIndexCache.put(value, filePosition);
                            position += str.getBytes().length + 1;
                        }
                    }
                }
                fileBeginNo++;
                System.out.println("good hash FIle " + count++);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (good_br != null) {
                    try {
                        good_br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public void run() {
        try {
            waitCountDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long startTime = System.currentTimeMillis();
        hash();
        countDownLatch.countDown();
        System.out
                .printf("GoodHasher work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis()
                                - OrderSystemImpl.constructStartTime);
    }
}
