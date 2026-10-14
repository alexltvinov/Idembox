package io.github.alexlitvinov.idembox.starter.util;

import org.apache.kafka.common.utils.KafkaThread;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ThreadTypeClassifierTest {

    private static Thread namedThread(String name) {
        return new Thread(() -> {}, name);
    }

    @Test
    @DisplayName("isKafkaNetworkThread detects actual Kafka client network threads")
    void shouldDetectKafkaNetworkThreads() {
        KafkaThread nativeKafkaThread = KafkaThread.nonDaemon("custom-sender-thread", () -> {});
        Thread producerThread = namedThread("kafka-producer-network-thread | producer-1");
        Thread coordinatorThread = namedThread("kafka-coordinator-heartbeat-thread | group-1");
        Thread consumerNetworkThread = namedThread("kafka-consumer-network-thread | client-2");

        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(nativeKafkaThread)).isTrue();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(producerThread)).isTrue();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(coordinatorThread)).isTrue();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(consumerNetworkThread)).isTrue();
    }

    @Test
    @DisplayName("isKafkaNetworkThread does not falsely classify Spring @KafkaListener or custom threads as Kafka network threads")
    void shouldNotFalselyClassifySpringKafkaListenerOrCustomThreads() {
        Thread springKafkaContainerThread = namedThread("org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1");
        Thread customListenerThread = namedThread("order-kafka-listener-1");
        Thread consumerPoolThread = namedThread("kafka-consumer-pool-2");
        Thread webOrderProcessor = namedThread("web-order-processor");
        Thread normalThread = namedThread("app-worker-1");

        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(springKafkaContainerThread)).isFalse();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(customListenerThread)).isFalse();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(consumerPoolThread)).isFalse();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(webOrderProcessor)).isFalse();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(normalThread)).isFalse();
        assertThat(ThreadTypeClassifier.isKafkaNetworkThread(null)).isFalse();
    }

    @Test
    @DisplayName("isWebContainerThread detects servlet container request threads")
    void shouldDetectWebContainerThreads() {
        Thread tomcatNio = namedThread("http-nio-8080-exec-1");
        Thread tomcatHttps = namedThread("https-nio-8443-exec-5");
        Thread catalina = namedThread("catalina-exec-2");
        Thread jetty = namedThread("qtp12345678-23");
        Thread undertow = namedThread("default task-1");
        Thread undertowWorker = namedThread("XN" + "IO-1 task-2");
        Thread grizzly = namedThread("grizzly-http-server-1");
        Thread reactorHttp = namedThread("reactor-http-epoll-1");

        assertThat(ThreadTypeClassifier.isWebContainerThread(tomcatNio)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(tomcatHttps)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(catalina)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(jetty)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(undertow)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(undertowWorker)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(grizzly)).isTrue();
        assertThat(ThreadTypeClassifier.isWebContainerThread(reactorHttp)).isTrue();
    }

    @Test
    @DisplayName("isWebContainerThread does not falsely classify application worker threads (web-order-processor, http-client)")
    void shouldNotFalselyClassifyApplicationThreadsAsWebThreads() {
        Thread webOrderProcessor = namedThread("web-order-processor");
        Thread httpClient = namedThread("http-client-worker-1");
        Thread nioFileScanner = namedThread("nio-file-scanner");
        Thread customThread = namedThread("custom-service-pool-1");

        assertThat(ThreadTypeClassifier.isWebContainerThread(webOrderProcessor)).isFalse();
        assertThat(ThreadTypeClassifier.isWebContainerThread(httpClient)).isFalse();
        assertThat(ThreadTypeClassifier.isWebContainerThread(nioFileScanner)).isFalse();
        assertThat(ThreadTypeClassifier.isWebContainerThread(customThread)).isFalse();
        assertThat(ThreadTypeClassifier.isWebContainerThread(null)).isFalse();
    }

    @Test
    @DisplayName("isEventLoopOrNonBlockingThread detects Netty and Reactor event loops while ignoring normal threads")
    void shouldDetectEventLoopThreads() {
        Thread nettyThread = namedThread("default-eventLoopGroup-2-1");
        Thread epollThread = namedThread("epollEventLoopGroup-3-2");
        Thread reactorThread = namedThread("reactor-http-epoll-4");
        Thread normalThread = namedThread("custom-worker-thread-1");
        Thread webOrderProcessor = namedThread("web-order-processor");

        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(nettyThread)).isTrue();
        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(epollThread)).isTrue();
        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(reactorThread)).isTrue();
        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(normalThread)).isFalse();
        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(webOrderProcessor)).isFalse();
        assertThat(ThreadTypeClassifier.isEventLoopOrNonBlockingThread(null)).isFalse();
    }
}
