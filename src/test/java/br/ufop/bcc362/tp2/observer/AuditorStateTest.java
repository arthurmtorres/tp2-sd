package br.ufop.bcc362.tp2.observer;

import br.ufop.bcc362.tp2.model.AuditEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditorStateTest {
    @Test
    void detectsOverlappingCriticalSectionsWithoutBlockingThem() {
        AuditorState state = new AuditorState();

        AuditorState.Result first = state.register(event(AuditEvent.Type.BEGIN, "node-1", "client-1-1"));
        AuditorState.Result second = state.register(event(AuditEvent.Type.BEGIN, "node-2", "client-2-1"));

        assertTrue(first.accepted());
        assertFalse(first.overlap());
        assertEquals(1, first.sequence());
        assertTrue(second.accepted());
        assertTrue(second.overlap());
        assertEquals(2, second.sequence());
        assertEquals(2, state.snapshot().maximumConcurrentAccesses());
        assertEquals(1, state.snapshot().overlapViolations());

        state.register(event(AuditEvent.Type.END, "node-1", "client-1-1"));
        state.register(event(AuditEvent.Type.END, "node-2", "client-2-1"));
        assertEquals(2, state.snapshot().completedAccesses());
        assertEquals(0, state.snapshot().activeAccesses());
    }

    @Test
    void rejectsEndWithoutMatchingBegin() {
        AuditorState state = new AuditorState();

        AuditorState.Result result = state.register(event(AuditEvent.Type.END, "node-1", "unknown-1"));

        assertFalse(result.accepted());
        assertEquals(1, state.snapshot().invalidEvents());
    }

    private AuditEvent event(AuditEvent.Type type, String processId, String requestId) {
        return new AuditEvent(type, processId, requestId, 1);
    }
}

