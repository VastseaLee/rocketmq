package com.young.springbootmq.controller;

import com.young.springbootmq.model.User;
import com.young.springbootmq.service.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("producer")
public class ProducerController {

    @Autowired
    private ProducerService producerService;

    @PostMapping("send")
    public void send(String topic, String msg) {
        producerService.sendMessage(topic, msg);
    }

    @PostMapping("sendUser")
    public void sendUser(String topic, User user) {
        producerService.sendUser(topic, user);
    }
}
