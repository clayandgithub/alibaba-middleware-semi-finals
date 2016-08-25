/**
 * RandomAccessFileUtil.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

import com.alibaba.middleware.race.Config;

/**
 * @author wangweiwei
 *
 */
public class RandomAccessFileUtil {
    public static String readLine(RandomAccessFile raf, long offset)
            throws IOException {
        raf.seek(offset);
        long pos = offset;
        long length = raf.length();
        LinkedList<byte[]> bytesList = new LinkedList<byte[]>();
        int lineLength = 0;
        while (pos < length) {
            byte[] buffer = new byte[Config.RAF_READLINE_BUFFER_SIZE];
            int readLength = raf.read(buffer);
            pos += readLength;
            int eolIndex = -1;
            for (int i = 0; i < buffer.length; ++i) {
                if (buffer[i] == '\n' || buffer[i] == '\r' || buffer[i] == -1) {
                    eolIndex = i;
                    break;
                }
            }
            if (eolIndex < 0) {
                lineLength += readLength;
                bytesList.add(buffer);
            } else if (eolIndex == 0) {
                lineLength += 1;
                break;
            } else {
                lineLength += (eolIndex + 1);
                byte[] b = subBytes(buffer, 0, eolIndex);
                if (b != null) {
                    bytesList.add(b);
                }
                break;
            }
        }
        if (lineLength > 0) {
            if (lineLength == 1) {
                return "";
            } else {
                byte[] lineBuffer = new byte[lineLength - 1];
                int tmpPos = 0;
                for (byte[] b : bytesList) {
                    System.arraycopy(b, 0, lineBuffer, tmpPos, b.length);
                    tmpPos += b.length;
                }
                return new String(lineBuffer);
            }
        } else {
            return null;
        }
    }

    public static byte[] subBytes(byte[] src, int beginIndex, int length) {
        if (length <= 0) {
            return null;
        }
        byte[] ret = new byte[length];
        System.arraycopy(src, beginIndex, ret, 0, length);
        return ret;
    }
}
