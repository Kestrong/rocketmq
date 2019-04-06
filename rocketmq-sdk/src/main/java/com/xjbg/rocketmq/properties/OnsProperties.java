package com.xjbg.rocketmq.properties;

import lombok.Getter;
import lombok.Setter;

/**
 * @author kesc
 * @since 2019/4/3
 */
@Getter
@Setter
public class OnsProperties {
    /**
     * ons的accessKey
     */
    private String accessKey;

    /**
     * ons的secretKey
     */
    private String secretKey;

    /**
     * 消息队列服务接入点
     */
    private String onsAddr;
    /**
     * regionId,默认为华南
     */
    private String regionId = "cn-shenzhen";

    /**
     * regionName,默认为华南
     */
    private String regionName = "华南 1";

    /**
     * endPoint
     */
    private String endPoint = "ons.cn-shenzhen.aliyuncs.com";
}
