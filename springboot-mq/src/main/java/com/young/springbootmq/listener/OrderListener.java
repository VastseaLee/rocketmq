//package com.young.springbootmq.listener;
//
//import org.apache.rocketmq.spring.annotation.ConsumeMode;
//import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
//import org.apache.rocketmq.spring.core.RocketMQListener;
//import org.springframework.messaging.Message;
//import org.springframework.stereotype.Component;
//
//@Component
//@RocketMQMessageListener(consumerGroup = "springbootGroup",topic = "orderTopic",consumeMode = ConsumeMode.ORDERLY)
//public class OrderListener implements RocketMQListener<Message<String>> {
//
//    @Override
//    public void onMessage(Message<String> message) {
//
//    }
//}
