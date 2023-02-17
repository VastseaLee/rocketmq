package com.young.springbootmq.listener;

import com.young.springbootmq.model.User;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.springframework.stereotype.Component;

@Component
@RocketMQMessageListener(consumerGroup = "springbootGroup",topic = "objectTopic")
public class ObjectListener implements RocketMQListener<User> {

    @Override
    public void onMessage(User message) {
        System.out.println("Received message:" + message);

    }
}
