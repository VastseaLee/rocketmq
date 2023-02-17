package com.example.scstreammq.controller;

import com.example.scstreammq.service.SendService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("sc")
public class SendController {

    @Autowired
    private SendService service;

    @GetMapping("send")
    public void send(String msg){
        service.send(msg);
    }
}
