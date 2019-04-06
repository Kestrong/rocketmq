package com.xjbg.rocketmq.util;

import com.xjbg.rocketmq.MqConstants;
import org.apache.commons.lang3.StringUtils;

/**
 * @author kesc
 * @since 2019/4/6
 */
public class ProfileUtil {
    /**
     * set once and remove after get*Message()
     */
    private static final ThreadLocal<String> TOPIC_PROFILE = new ThreadLocal<>();

    private static String topicPrefix = "";

    public static void setTopicProfile(String topicProfile) {
        if (StringUtils.isNotBlank(topicProfile)) {
            ProfileUtil.topicPrefix = topicProfile + MqConstants.DASH;
        }
    }

    public static void setTopicProfileOnce(String topicProfile) {
        TOPIC_PROFILE.set(topicProfile);
    }

    public static String getTopicPrefix() {
        if (TOPIC_PROFILE.get() != null) {
            return TOPIC_PROFILE.get() + MqConstants.DASH;
        }
        return topicPrefix;
    }

    public static void remove() {
        TOPIC_PROFILE.remove();
    }
}
