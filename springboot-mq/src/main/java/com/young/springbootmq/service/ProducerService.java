package com.young.springbootmq.service;

import com.young.springbootmq.model.User;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class ProducerService {

    @Autowired
    private RocketMQTemplate rocketMQTemplate;

    public void sendMessage(String topic, String msg) {
        rocketMQTemplate.convertAndSend(topic, msg);
//        rocketMQTemplate.convertAndSend(topic + ":tagA", msg);
        System.out.println("[send][topic]" + topic + "[msg]" + msg);
    }

    public void synSend(String topic, String msg) {
        rocketMQTemplate.syncSend(topic, msg);
    }

    public void asynSend(String topic, String msg) {
        rocketMQTemplate.asyncSend(topic, msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {

            }

            @Override
            public void onException(Throwable throwable) {

            }
        });
    }

    public void orderSend(){
        rocketMQTemplate.syncSendOrderly("topic","","orderId");
    }

    public void sendUser(String topic, User user) {
        Message<User> message = MessageBuilder.withPayload(user).build();
        rocketMQTemplate.syncSend(topic, message);
    }

    public void sendTransaction(String topic, User user) {
        Message<User> message = MessageBuilder.withPayload(user).build();
        rocketMQTemplate.sendMessageInTransaction(topic, message,new Object());
    }
}
