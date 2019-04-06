package com.xjbg.rocketmq.filter;

/**
 * @author kesc
 * @since 2019/4/4
 */
public interface MessageRepeatFilter {

    /**
     * check repeat msg
     *
     * @param msgKey unique message key
     * @return
     */
    boolean isRepeat(String msgKey);

    /**
     * reset
     *
     * @param msgKey
     * @return
     */
    void reset(String msgKey);
}
