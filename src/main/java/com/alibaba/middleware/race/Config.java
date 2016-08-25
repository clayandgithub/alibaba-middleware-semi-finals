/**
 * Config.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race;

/**
 * @author wangweiwei
 *
 */
public class Config {

    public static final int ORDER_ONE_INDEX_FILE_NUMBER = 3000;

    public static final int GOOD_HASH_FILE_NUM = 2000;

    public static final int BUYER_HASH_FILE_NUM = 2000;

    public static final int MAX_CONCURRENT = 200;

    public static final int SWITCH_THREAD_SLEEP_TIME = 3580000;

    public static final int RAF_READLINE_BUFFER_SIZE = 1024;

    public static final int ORDER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM = 6;

    public static final int BUYER_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM = 6;

    public static final int GOOD_ID_TWO_INDEX_BUILDER_MAX_CONCURRENT_NUM = 8;

    public static String FIRST_DISK_PATH = "";

    public static String SECOND_DISK_PATH = "";

    public static String THIRD_DISK_PATH = "";

}
