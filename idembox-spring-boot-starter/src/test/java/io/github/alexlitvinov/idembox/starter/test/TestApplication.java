package io.github.alexlitvinov.idembox.starter.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.Mockito.mock;

@SpringBootApplication
public class TestApplication {

    @Bean
    @SuppressWarnings("unchecked")
    public KafkaTemplate<String, String> kafkaTemplate() {
        return mock(KafkaTemplate.class);
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        return mapper;
    }
}
