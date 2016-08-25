package com.alibaba.middleware.race.order;

import com.alibaba.middleware.race.Config;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.cache.FileNameCache;
import com.alibaba.middleware.race.constant.FileConstant;
import com.alibaba.middleware.race.constant.IndexConstant;

import java.io.*;
import java.util.Collection;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

/**
 * 根据order文件生成orderid一级索引文件(未排序)并存储
 * 
 * 存放位置：第一个硬盘
 * 
 * @author jiangchao
 */
public class OrderIdOneIndexBuilder extends Thread {

    private Collection<String> orderFiles;
    private int indexFileNum;
    private CountDownLatch builderLatch;
    private int fileBeginNo;

    public OrderIdOneIndexBuilder(Collection<String> orderFiles, int indexFileNum, CountDownLatch builderLatch, int fileBeginNum) {
        this.orderFiles = orderFiles;
        this.indexFileNum = indexFileNum;
        this.builderLatch = builderLatch;
        this.fileBeginNo = fileBeginNum;
    }

    // 读取所有订单文件，为每条记录生成orderid索引并按照订单号hash到对应的索引文件
    public void build() {
        try {
            BufferedWriter[] bufferedWriters = new BufferedWriter[indexFileNum];
            for (int i = 0; i < indexFileNum; i++) {
                bufferedWriters[i] = new BufferedWriter(
                        new FileWriter(
                                Config.FIRST_DISK_PATH
                                        + FileConstant.UNSORTED_ORDER_ID_ONE_INDEX_FILE_PREFIX
                                        + i));
            }

            // 每个orderFile 分配一个task
            CountDownLatch tasksLatch = new CountDownLatch(orderFiles.size());
            for (String orderFile : orderFiles) {
                FileNameCache.fileNameMap.put(fileBeginNo, orderFile);
                new SingleFileBuildTask(orderFile, tasksLatch, bufferedWriters, fileBeginNo).start();
                fileBeginNo++;
            }
            tasksLatch.await();

            for (int i = 0; i < indexFileNum; i++) {
                bufferedWriters[i].close();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        build();
        builderLatch.countDown();
        System.out
                .printf("OrderIdOneIndexBuilder work end! Used time：%d End time : %d %n",
                        System.currentTimeMillis() - startTime,
                        System.currentTimeMillis() - OrderSystemImpl.constructStartTime);
    }

    private class SingleFileBuildTask extends Thread {
        private String orderFile;
        private CountDownLatch tasksLatch;
        private BufferedWriter[] bufferedWriters;
        private int fileNum;

        public SingleFileBuildTask(String orderFile, CountDownLatch tasksLatch, BufferedWriter[] bufferedWriters, int fileNum) {
            this.orderFile = orderFile;
            this.tasksLatch = tasksLatch;
            this.bufferedWriters = bufferedWriters;
            this.fileNum = fileNum;
        }

        @Override
        public void run() {
            try {
                FileInputStream orderRecords = new FileInputStream(orderFile);
                BufferedReader orderBr = new BufferedReader(new InputStreamReader(orderRecords));

                String line = null;
                long position = 0;
                while ((line = orderBr.readLine()) != null) {
                    StringTokenizer stringTokenizer = new StringTokenizer(line, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        if (IndexConstant.ORDER_ID.equals(key)) {
                            Long orderId = Long.valueOf(value);
                            int hashFileIndex = (int) (orderId % indexFileNum);
                            String indexLine = orderId + ":" + fileNum + ":" + position + '\n';
                            synchronized (bufferedWriters[hashFileIndex]) {
                                bufferedWriters[hashFileIndex].write(indexLine);
                            }
                            position += (line.getBytes().length + 1);
                            break;
                        }
                    }
                }
                orderBr.close();
                tasksLatch.countDown();
                System.out.println(IndexConstant.ORDER_ID + " SingleFileBuildTask end :" + orderFile);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
