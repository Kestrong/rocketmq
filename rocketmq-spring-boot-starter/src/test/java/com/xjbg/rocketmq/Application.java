package com.xjbg.rocketmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author kesc
 * @since 2019/4/5
 */
@SpringBootApplication(scanBasePackages = "com.xjbg.rocketmq")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}
