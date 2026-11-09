package br.ufop.bcc362.tp2;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationTest {
    @Test
    void serializesRequest() throws Exception {
        WireProtocol.AccessRequest expected = new WireProtocol.AccessRequest("client-3", 7, 450);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        WireProtocol.writeRequest(new DataOutputStream(bytes), expected);

        WireProtocol.AccessRequest actual = WireProtocol.readRequest(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(expected, actual);
    }

    @Test
    void serializesResponse() throws Exception {
        WireProtocol.AccessResponse expected = WireProtocol.AccessResponse.committed("client-3-7", 123_456);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        WireProtocol.writeResponse(new DataOutputStream(bytes), expected);

        WireProtocol.AccessResponse actual = WireProtocol.readResponse(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(expected, actual);
    }

    @Test
    void observerDetectsOverlap() {
        ObserverServer.AuditorState state = new ObserverServer.AuditorState();
        ObserverServer.Result first = state.register(event(WireProtocol.AuditEvent.Type.BEGIN,
                "node-1", "client-1-1"));
        ObserverServer.Result second = state.register(event(WireProtocol.AuditEvent.Type.BEGIN,
                "node-2", "client-2-1"));

        assertTrue(first.accepted());
        assertFalse(first.overlap());
        assertEquals(1, first.sequence());
        assertTrue(second.overlap());
        assertEquals(2, second.sequence());
        assertEquals(2, state.snapshot().maxConcurrent());
        assertEquals(1, state.snapshot().overlaps());

        state.register(event(WireProtocol.AuditEvent.Type.END, "node-1", "client-1-1"));
        state.register(event(WireProtocol.AuditEvent.Type.END, "node-2", "client-2-1"));
        assertEquals(2, state.snapshot().completed());
        assertEquals(0, state.snapshot().active());
    }

    @Test
    void observerRejectsEndWithoutBegin() {
        ObserverServer.AuditorState state = new ObserverServer.AuditorState();
        ObserverServer.Result result = state.register(
                event(WireProtocol.AuditEvent.Type.END, "node-1", "unknown-1"));

        assertFalse(result.accepted());
        assertEquals(1, state.snapshot().invalid());
    }

    private WireProtocol.AuditEvent event(WireProtocol.AuditEvent.Type type,
                                          String processId, String requestId) {
        return new WireProtocol.AuditEvent(type, processId, requestId, 1);
    }
}

