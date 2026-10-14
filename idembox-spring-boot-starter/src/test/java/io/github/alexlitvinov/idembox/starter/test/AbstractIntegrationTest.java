package io.github.alexlitvinov.idembox.starter.test;

import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

@SpringBootTest(classes = TestApplication.class, properties = {
        "idembox.outbox.poller.enabled=false",
        "idembox.scheduling.enabled=false"
})
public abstract class AbstractIntegrationTest {

    protected static PostgreSQLContainer<?> postgres;
    protected static final boolean DOCKER_AVAILABLE;

    static {
        boolean available;
        try {
            available = DockerClientFactory.instance().isDockerAvailable();
        } catch (Throwable ignored) {
            // Testcontainers can throw InvalidPathException on Windows if PATH contains quotes
            available = false;
        }
        DOCKER_AVAILABLE = available;

        if (DOCKER_AVAILABLE) {
            try {
                postgres = createPostgresContainer();
                postgres.start();
            } catch (Throwable t) {
                postgres = null;
            }
        }
    }

    private static PostgreSQLContainer<?> createPostgresContainer() {
        try {
            PostgreSQLContainer<?> container = (PostgreSQLContainer<?>) PostgreSQLContainer.class
                    .getConstructor(String.class)
                    .newInstance("postgres:16-alpine");
            container.withDatabaseName("test_idembox_db")
                    .withUsername("test_user")
                    .withPassword("test_pass")
                    .withInitScript("db/migration/idembox/schema-postgresql.sql");
            return container;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to instantiate PostgreSQLContainer", e);
        }
    }

    @BeforeAll
    static void checkDocker() {
        assumeTrue(
                DOCKER_AVAILABLE && postgres != null && postgres.isRunning(),
                "Docker is not available or could not be started. Skipping Testcontainers integration test."
        );
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("idembox.outbox.poller.enabled", () -> "false");
        registry.add("idembox.scheduling.enabled", () -> "false");
        if (postgres != null && postgres.isRunning()) {
            registry.add("spring.datasource.url", postgres::getJdbcUrl);
            registry.add("spring.datasource.username", postgres::getUsername);
            registry.add("spring.datasource.password", postgres::getPassword);
            registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        } else {
            // Fallback in-memory datasource to allow Spring context initialization when tests are skipped
            registry.add("spring.datasource.url", () -> "jd" + "bc:h2:mem:test_db;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1");
            registry.add("spring.datasource.driver-class-name", () -> "org.h2.Driver");
            registry.add("spring.datasource.username", () -> "sa");
            registry.add("spring.datasource.password", () -> "");
        }
    }
}
