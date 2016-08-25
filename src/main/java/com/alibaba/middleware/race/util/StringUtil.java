package com.alibaba.middleware.race.util;

import java.util.Random;

public class StringUtil {
    private static final String RANDOM_STRING_BASE = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    public static String genRandomString(final int length) {
        final Random random = new Random();
        final StringBuilder retSb = new StringBuilder();
        final int baseLength = RANDOM_STRING_BASE.length();
        for (int i = 0; i < length; ++i) {
            char c = RANDOM_STRING_BASE.charAt(random.nextInt(baseLength));
            retSb.append(c);
        }
        return retSb.toString();
    }

    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
