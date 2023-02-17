//package com.young.springbootmq.listener;
//
//import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
//import org.apache.rocketmq.spring.core.RocketMQListener;
//import org.springframework.stereotype.Component;
//
//@Component
//@RocketMQMessageListener(consumerGroup = "springbootGroup",topic = "YoungTopic")
//public class MyMQListener implements RocketMQListener<String> {
//
//    @Override
//    public void onMessage(String s) {
//        System.out.println("Received message:" + s);
//    }
//
//
//}
