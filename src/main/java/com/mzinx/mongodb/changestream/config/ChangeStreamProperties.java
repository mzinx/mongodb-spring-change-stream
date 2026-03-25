package com.mzinx.mongodb.changestream.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Data
@ConfigurationProperties("change-stream")
@Component
public class ChangeStreamProperties {
    private boolean enabled = true;
    private String hostname = System.getenv().getOrDefault("HOSTNAME", "localhost");
    private long batchSize = 1000;
    private long maxAwaitTime = 800; // ms
    private long tokenMaxLifeTime = 86400000; // ms
    private long maxTimeout = 5000*10; // ms
    private String resumeTokenCollection = "_resumeTokens";
    private String instanceCollection = "_instances";
    private String changeStreamCollection = "_changeStreams";
}
