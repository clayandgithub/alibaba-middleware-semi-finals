package com.alibaba.middleware.race.buyer;

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
 * 根据buyerid生成buyer的索引并缓存 Created by jiangchao on 2016/7/13.
 */
public class BuyerIndexBuilder extends Thread {
    private CountDownLatch waitCountDownLatch;
    private Collection<String> buyerFiles;
    private CountDownLatch countDownLatch;
    private int fileBeginNo;

    public BuyerIndexBuilder(CountDownLatch waitCountDownLatch,
            Collection<String> buyerFiles, CountDownLatch countDownLatch,
            int fileBeginNo) {
        this.waitCountDownLatch = waitCountDownLatch;
        this.buyerFiles = buyerFiles;
        this.countDownLatch = countDownLatch;
        this.fileBeginNo = fileBeginNo;
    }

    public void hash() {

        int count = 0;
        for (String buyerFile : buyerFiles) {
            FileNameCache.fileNameMap.put(fileBeginNo, buyerFile);
            BufferedReader buyer_br = null;
            try {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                long position = 0;
                while ((str = buyer_br.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (!KeyCache.buyerKeyCache.containsKey(key)) {
                            KeyCache.buyerKeyCache.put(key, 0);
                        }
                        if (IndexConstant.BUYER_ID.equals(key)) {
                            FilePosition filePosition = new FilePosition(fileBeginNo, position);
                            OneIndexCache.buyerOneIndexCache.put(value, filePosition);
                            position += str.getBytes().length + 1;
                        }
                    }
                }
                System.out.println("buyer hash file " + count++);
                fileBeginNo++;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (buyer_br != null) {
                    try {
                        buyer_br.close();
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
                .printf("BuyerHasher work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis()
                                - OrderSystemImpl.constructStartTime);
    }

}
