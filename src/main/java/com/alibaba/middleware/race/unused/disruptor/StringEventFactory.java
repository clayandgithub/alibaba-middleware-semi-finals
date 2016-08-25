/**
 * StringEventFactory.java
 * Copyright 2016 escenter@zju.edu.cn, all rights reserved.
 * any form of usage is subject to approval.
 */
package com.alibaba.middleware.race.unused.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * @author wangweiwei
 *
 */
public class StringEventFactory implements EventFactory<StringEvent>{

    @Override
    public StringEvent newInstance() {
        return new StringEvent();
    }

}
