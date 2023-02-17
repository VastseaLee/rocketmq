package com.example.scstreammq.service;

import org.apache.rocketmq.common.message.MessageConst;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.cloud.stream.messaging.Source;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class SendService {

    @Autowired
    private StreamBridge streamBridge;

    public void send(String msg) {
//        Map<String,Object> header = new HashMap<>();
////        header.put(MessageConst.PROPERTY_TAGS,"");
//        MessageHeaders messageHeaders = new MessageHeaders(header);
//        Message<String> message = MessageBuilder.createMessage(msg,messageHeaders);
//        System.out.println("send msg:" + msg);

        streamBridge.send("scTopic", msg);
        System.out.println("send msg:" + msg);
    }
}
