package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.cache.BuyerCache;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/7/27.
 */
public class SwitchThread extends Thread {

    private CountDownLatch countDownLatch;

    private Collection<String> buyerFiles;

    private CountDownLatch buildIndexCountDownLatch;

    public SwitchThread(CountDownLatch countDownLatch,
            Collection<String> buyerFiles,
            CountDownLatch buildIndexCountDownLatch) {
        this.countDownLatch = countDownLatch;
        this.buyerFiles = buyerFiles;
        this.buildIndexCountDownLatch = buildIndexCountDownLatch;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(Config.SWITCH_THREAD_SLEEP_TIME);
            BuyerCache buyerCache = new BuyerCache(buyerFiles,
                    buildIndexCountDownLatch);
            buyerCache.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        countDownLatch.countDown();
    }
}
