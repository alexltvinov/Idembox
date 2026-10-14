package io.github.alexlitvinov.idembox.starter.cleaner;

import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyCleanerTest {

    @Mock
    private IdempotentRepository idempotentRepository;

    @Test
    @DisplayName("Should execute cleanup when idempotency is enabled")
    void shouldExecuteCleanupWhenEnabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(true);

        when(idempotentRepository.executeWithLock(any(), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });
        when(idempotentRepository.cleanupExpired(any(Instant.class), any(java.time.Duration.class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(42);

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(42);
        verify(idempotentRepository).executeWithLock(org.mockito.ArgumentMatchers.eq("idembox_idempotency_cleanup"), any());
        verify(idempotentRepository).cleanupExpired(any(Instant.class), any(java.time.Duration.class), org.mockito.ArgumentMatchers.eq(5000), org.mockito.ArgumentMatchers.eq(100));
    }

    @Test
    @DisplayName("Should skip cleanup when idempotency is disabled")
    void shouldSkipCleanupWhenDisabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(false);

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(0);
        verify(idempotentRepository, never()).executeWithLock(any(), any());
        verify(idempotentRepository, never()).cleanupExpired(any());
        verify(idempotentRepository, never()).cleanupExpired(any(), any(), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    @DisplayName("Senior Refinements v24: Should skip cleanup when cleaner is explicitly disabled")
    void shouldSkipCleanupWhenCleanerDisabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(true);
        properties.getIdempotency().getCleaner().setEnabled(false);

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(0);
        verify(idempotentRepository, never()).executeWithLock(any(), any());
        verify(idempotentRepository, never()).cleanupExpired(any());
        verify(idempotentRepository, never()).cleanupExpired(any(), any(), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
    }


    @Test
    @DisplayName("Senior Refinements v14: Should skip cleanup when advisory lock is held by another cluster node")
    void shouldSkipCleanupWhenAdvisoryLockHeldByAnotherNode() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(true);

        when(idempotentRepository.executeWithLock(any(), any())).thenReturn(false);

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(0);
        verify(idempotentRepository).executeWithLock(org.mockito.ArgumentMatchers.eq("idembox_idempotency_cleanup"), any());
        verify(idempotentRepository, never()).cleanupExpired(any());
        verify(idempotentRepository, never()).cleanupExpired(any(), any(), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    @DisplayName("Should handle database exception inside cleanupExpired gracefully without throwing")
    void shouldHandleDatabaseExceptionGracefully() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(true);

        when(idempotentRepository.executeWithLock(any(), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });
        when(idempotentRepository.cleanupExpired(any(Instant.class), any(java.time.Duration.class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new org.springframework.dao.QueryTimeoutException("PostgreSQL lock timeout"));

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(0);
        verify(idempotentRepository).executeWithLock(org.mockito.ArgumentMatchers.eq("idembox_idempotency_cleanup"), any());
        verify(idempotentRepository).cleanupExpired(any(Instant.class), any(java.time.Duration.class), org.mockito.ArgumentMatchers.eq(5000), org.mockito.ArgumentMatchers.eq(100));
    }

    @Test
    @DisplayName("Senior Refinements Round 68: Cleaner passes configured batchSize and maxBatchesPerRun to repository")
    void shouldPassConfiguredBatchSizeAndMaxBatchesToRepository() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getIdempotency().setEnabled(true);
        properties.getIdempotency().getCleaner().setBatchSize(2500);
        properties.getIdempotency().getCleaner().setMaxBatchesPerRun(50);

        when(idempotentRepository.executeWithLock(any(), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });
        when(idempotentRepository.cleanupExpired(any(), any(), org.mockito.ArgumentMatchers.eq(2500), org.mockito.ArgumentMatchers.eq(50))).thenReturn(150);

        IdempotencyCleaner cleaner = new IdempotencyCleaner(idempotentRepository, properties);
        int deleted = cleaner.cleanupExpiredRecords();

        assertThat(deleted).isEqualTo(150);
        verify(idempotentRepository).cleanupExpired(any(), any(), org.mockito.ArgumentMatchers.eq(2500), org.mockito.ArgumentMatchers.eq(50));
    }

    @Test
    @DisplayName("Senior Refinements v37: cleanupExpiredRecords specifies idemboxTaskScheduler")
    void shouldSpecifyIdemboxTaskScheduler() throws Exception {
        java.lang.reflect.Method method = IdempotencyCleaner.class.getMethod("cleanupExpiredRecords");
        org.springframework.scheduling.annotation.Scheduled scheduled = method.getAnnotation(org.springframework.scheduling.annotation.Scheduled.class);
        assertThat(scheduled).isNotNull();
        assertThat(scheduled.scheduler()).isEqualTo("idemboxTaskScheduler");
    }
}
