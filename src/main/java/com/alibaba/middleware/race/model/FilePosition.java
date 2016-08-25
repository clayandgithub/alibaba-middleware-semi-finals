package com.alibaba.middleware.race.model;

/**
 * Created by jiangchao on 2016/7/31.
 */
public class FilePosition {

    private int fileNum;
    private long position;

    public FilePosition(int fileNum, long position) {
        this.fileNum = fileNum;
        this.position = position;
    }

    public int getFileNum() {
        return fileNum;
    }

    public void setFileNum(int fileNum) {
        this.fileNum = fileNum;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }
}
