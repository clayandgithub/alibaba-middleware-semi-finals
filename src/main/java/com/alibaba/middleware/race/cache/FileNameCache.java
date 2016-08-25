package com.alibaba.middleware.race.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jiangchao on 2016/8/1.
 */
public class FileNameCache {
    public static Map<Integer, String> fileNameMap = new ConcurrentHashMap<Integer, String>();
}
