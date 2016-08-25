package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import com.alibaba.middleware.race.buyer.*;
import com.alibaba.middleware.race.good.*;
import com.alibaba.middleware.race.order.*;
import com.alibaba.middleware.race.util.SwitchThread;

/**
 * Created by jiangchao on 2016/7/11.
 */
public class OrderSystemImpl implements OrderSystem {

    private static CountDownLatch buildIndexLatch = new CountDownLatch(
            3 * Config.ORDER_ONE_INDEX_FILE_NUMBER);
    private static CountDownLatch buyerIndexLatch = new CountDownLatch(1);
    private static CountDownLatch goodIndexLatch = new CountDownLatch(1);

    public static long constructStartTime = 0;

    @Override
    public void construct(final Collection<String> orderFiles,
            final Collection<String> buyerFiles,
            final Collection<String> goodFiles, Collection<String> storeFolders)
            throws IOException, InterruptedException {
        constructStartTime = System.currentTimeMillis();

        // 定时器线程启动
        CountDownLatch switchCountDownLatch = new CountDownLatch(1);
        SwitchThread switchThread = new SwitchThread(switchCountDownLatch,
                buyerFiles, buildIndexLatch);
        switchThread.start();

        // 设定存储路径
        if (storeFolders != null && storeFolders.size() >= 3) {
            Config.FIRST_DISK_PATH = Config.FIRST_DISK_PATH
                    + storeFolders.toArray()[0];
            Config.SECOND_DISK_PATH = Config.SECOND_DISK_PATH
                    + storeFolders.toArray()[1];
            Config.THIRD_DISK_PATH = Config.THIRD_DISK_PATH
                    + storeFolders.toArray()[2];
        }

        System.out.println("Begin to build index...");

        // 各种文件的起始编号
        int goodFilesBeginNo = 0;
        int buyerFilesBeginNo = goodFiles.toArray().length;
        int orderFilesBeginNo = buyerFilesBeginNo + buyerFiles.toArray().length;

        // 按买家ID建立订单的一级索引文件(未排序)
        CountDownLatch buyerIdOneIndexBuilderLatch = new CountDownLatch(1);
        BuyerIdHasher buyerIdOneIndexBuilder = new BuyerIdHasher(
                orderFiles, Config.ORDER_ONE_INDEX_FILE_NUMBER,
                buyerIdOneIndexBuilderLatch, orderFilesBeginNo);
        buyerIdOneIndexBuilder.start();

        // 按商品ID将订单hash成多个小文件(未排序)
        CountDownLatch goodIdHasherLatch = new CountDownLatch(1);
        GoodIdHasher goodIdHasher = new GoodIdHasher(orderFiles,
                Config.ORDER_ONE_INDEX_FILE_NUMBER, goodIdHasherLatch,
                orderFilesBeginNo);
        goodIdHasher.start();

        // 按orderid建立order的一级索引文件(未排序)
        CountDownLatch orderIdOneIndexBuilderLatch = new CountDownLatch(1);
        OrderIdOneIndexBuilder orderIdHashThread = new OrderIdOneIndexBuilder(
                orderFiles, Config.ORDER_ONE_INDEX_FILE_NUMBER,
                orderIdOneIndexBuilderLatch, orderFilesBeginNo);
        orderIdHashThread.start();

        // 根据orderid生成order的二级索引(同时生成排序的一级索引文件)
        OrderIdTwoIndexBuilder orderIdTwoIndexBuilder = new OrderIdTwoIndexBuilder(
                orderIdOneIndexBuilderLatch, buildIndexLatch,
                Config.ORDER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        orderIdTwoIndexBuilder.start();

        // 根据buyerid生成order的二级索引(同时生成排序的一级索引文件)
        BuyerIdIndexBuilder buyerIdIndexFile = new BuyerIdIndexBuilder(
                buyerIdOneIndexBuilderLatch, buildIndexLatch,
                Config.BUYER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        buyerIdIndexFile.start();

        // 根据goodid生成order的一级二级索引
        GoodIdIndexBuilder goodIdIndexBuilder = new GoodIdIndexBuilder(
                goodIdHasherLatch, buildIndexLatch,
                Config.GOOD_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM);
        goodIdIndexBuilder.start();

        // 根据goodid生成good的索引并缓存
        GoodIndexBuilder goodIndexBuilder = new GoodIndexBuilder(
                buildIndexLatch, goodFiles, goodIndexLatch, goodFilesBeginNo);
        goodIndexBuilder.start();

        // 根据buyerid生成buyer的索引并缓存
        BuyerIndexBuilder buyerIndexBuilder = new BuyerIndexBuilder(
                buildIndexLatch, buyerFiles, buyerIndexLatch, buyerFilesBeginNo);
        buyerIndexBuilder.start();

        // 等待定时器线程结束
        switchCountDownLatch.await();
        System.out.println("construct end time is :"
                + (System.currentTimeMillis() - constructStartTime));
    }

    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        try {
            buildIndexLatch.await();
            goodIndexLatch.await();
            buyerIndexLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return OrderIdQuery.findOrder(orderId, keys);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.model.Result> queryOrdersByBuyer(
            long startTime, long endTime, String buyerid) {
        try {
            buildIndexLatch.await();
            goodIndexLatch.await();
            buyerIndexLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return BuyerIdQuery.findOrdersByBuyer(startTime, endTime, buyerid);
    }

    @Override
    public Iterator<com.alibaba.middleware.race.model.Result> queryOrdersBySaler(
            String salerid, String goodid, Collection<String> keys) {
        try {
            buildIndexLatch.await();
            goodIndexLatch.await();
            buyerIndexLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return GoodIdQuery.findOrdersByGood(salerid, goodid, keys);
    }

    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        try {
            buildIndexLatch.await();
            goodIndexLatch.await();
            buyerIndexLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return GoodIdQuery.sumValuesByGood(goodid, key);
    }
}
