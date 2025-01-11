package com.nginx.stats.bytes;

import com.nginx.stats.bytes.streams.NginxStatsBytesRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(NginxStatsBytesRunner.class);
    }
}
