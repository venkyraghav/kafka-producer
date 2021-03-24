package com.venkyraghav.confluent.kafka.model;

import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class MyMessage {
    private String id;
    private String message;

    public MyMessage(String id, String message) {
        this.id = id;
        this.message = message;
    }
}