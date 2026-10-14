package io.github.alexlitvinov.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SampleOrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(SampleOrderApplication.class, args);
    }
}
