/**
 * RandomAccessFileUtilTest.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

import com.alibaba.middleware.race.util.RandomAccessFileUtil;
import com.alibaba.middleware.race.util.StringUtil;

/**
 * @author wangweiwei
 *
 */
public class RandomAccessFileUtilTest {

    @Test
    public void proceduceLargeFile() throws IOException {
        long startTime = System.currentTimeMillis();
        File file = new File("largeFile.txt");
        FileWriter fw = new FileWriter(file);
        BufferedWriter bufferedWriter = new BufferedWriter(fw);
        int lineCount = 0;
        while (lineCount++ < 50000) {
            String line = StringUtil.genRandomString(2054) + "中文" + "\n";
            bufferedWriter.write(line);
            if (lineCount == 1) {
                System.out.println(line);
                System.out.println(line.getBytes().length);
            }
        }
        bufferedWriter.flush();
        bufferedWriter.close();
        System.out.println("lineCount : " + lineCount);
        System.out.println("Total used time : "
                + (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testRandomAccessFileReadLine() throws IOException {
        long startTime = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        int lineCount = 0;
        while ((line = raf.readLine()) != null) {
            ++lineCount;
            if (lineCount == 1) {
                System.out.println(line);
                System.out.println(line.getBytes().length);
            }
        }
        raf.close();
        System.out.println("lineCount : " + lineCount);
        System.out.println("Total used time : "
                + (System.currentTimeMillis() - startTime));
    }

    @Test
    public void testRandomAccessFileUtilReadLine() throws IOException {
        long startTime = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("largeFile.txt", "r");
        String line = null;
        int lineCount = 0;
        long offset = 0;
        while ((line = RandomAccessFileUtil.readLine(raf, offset)) != null) {
            offset += (line.getBytes().length + 1);
            ++lineCount;
            if (lineCount == 1) {
                System.out.println(line);
                System.out.println(line.getBytes().length);
            }
        }
        raf.close();
        System.out.println("lineCount : " + lineCount);
        System.out.println("Total used time : "
                + (System.currentTimeMillis() - startTime));
    }
}
