/*
 * Created by lzy on 2018/11/28 5:52 PM.
 */
package com.xjbg.rocketmq.handler;

/**
 * @author kesc
 * @version v1.0
 */
@FunctionalInterface
public interface Handler<T> {
    /**
     * Accept.
     *
     * @param t the t
     * @throws Exception the exception
     */
    void handle(T t) throws Exception;
}
