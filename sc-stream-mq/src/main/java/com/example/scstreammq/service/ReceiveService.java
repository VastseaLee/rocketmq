package com.example.scstreammq.service;

import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.stereotype.Service;


@Service
public class ReceiveService implements Sink {

    @Override
    public SubscribableChannel input() {
        return null;
    }
}
