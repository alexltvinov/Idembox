package io.github.alexlitvinov.idembox.starter.cleaner;

import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxCleanerTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Test
    @DisplayName("Should execute cleanup when outbox and cleaner are enabled")
    void shouldExecuteCleanupWhenOutboxAndCleanerEnabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getCleaner().setEnabled(true);
        properties.getOutbox().setRetention(Duration.ofDays(7));
        properties.getOutbox().setFailedRetention(Duration.ofDays(30));

        when(outboxRepository.executeWithLock(eq("idembox_outbox_cleanup"), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });
        when(outboxRepository.deleteSentBefore(any(Instant.class), any(Duration.class), eq(5000), eq(100))).thenReturn(15);
        when(outboxRepository.deleteFailedBefore(any(Instant.class), any(Duration.class), eq(5000), eq(100))).thenReturn(3);

        OutboxCleaner cleaner = new OutboxCleaner(outboxRepository, properties);
        cleaner.cleanupOldMessages();

        verify(outboxRepository).executeWithLock(eq("idembox_outbox_cleanup"), any());
        verify(outboxRepository).deleteSentBefore(any(Instant.class), any(Duration.class), eq(5000), eq(100));
        verify(outboxRepository).deleteFailedBefore(any(Instant.class), any(Duration.class), eq(5000), eq(100));
    }

    @Test
    @DisplayName("Should skip cleanup when outbox is disabled")
    void shouldSkipCleanupWhenOutboxDisabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getOutbox().setEnabled(false);
        properties.getOutbox().getCleaner().setEnabled(true);

        OutboxCleaner cleaner = new OutboxCleaner(outboxRepository, properties);
        cleaner.cleanupOldMessages();

        verify(outboxRepository, never()).executeWithLock(any(), any());
        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }

    @Test
    @DisplayName("Should skip cleanup when cleaner is explicitly disabled")
    void shouldSkipCleanupWhenCleanerDisabled() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getCleaner().setEnabled(false);

        OutboxCleaner cleaner = new OutboxCleaner(outboxRepository, properties);
        cleaner.cleanupOldMessages();

        verify(outboxRepository, never()).executeWithLock(any(), any());
        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }

    @Test
    @DisplayName("Should skip cleanup when advisory lock is held by another cluster node")
    void shouldSkipCleanupWhenLockCannotBeAcquired() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getCleaner().setEnabled(true);

        when(outboxRepository.executeWithLock(eq("idembox_outbox_cleanup"), any())).thenReturn(false);

        OutboxCleaner cleaner = new OutboxCleaner(outboxRepository, properties);
        cleaner.cleanupOldMessages();

        verify(outboxRepository).executeWithLock(eq("idembox_outbox_cleanup"), any());
        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }

    @Test
    @DisplayName("Should handle database exceptions during cleanup gracefully without throwing")
    void shouldHandleDatabaseExceptionGracefully() {
        IdemboxProperties properties = new IdemboxProperties();
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getCleaner().setEnabled(true);
        properties.getOutbox().setRetention(Duration.ofDays(7));

        when(outboxRepository.executeWithLock(eq("idembox_outbox_cleanup"), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });
        when(outboxRepository.deleteSentBefore(any(Instant.class), any(Duration.class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new RuntimeException("DB deadlock"));

        OutboxCleaner cleaner = new OutboxCleaner(outboxRepository, properties);
        // Should not throw exception
        cleaner.cleanupOldMessages();

        verify(outboxRepository).executeWithLock(eq("idembox_outbox_cleanup"), any());
    }
}
