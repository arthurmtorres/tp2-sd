package br.ufop.bcc362.tp2.protocol;

import br.ufop.bcc362.tp2.model.AccessRequest;
import br.ufop.bcc362.tp2.model.AccessResponse;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WireProtocolTest {
    @Test
    void roundTripsAccessRequest() throws Exception {
        AccessRequest expected = new AccessRequest("client-3", 7, 450);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        WireProtocol.writeRequest(new DataOutputStream(bytes), expected);
        AccessRequest actual = WireProtocol.readRequest(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(expected, actual);
    }

    @Test
    void roundTripsAccessResponse() throws Exception {
        AccessResponse expected = AccessResponse.committed("client-3-7", 123_456);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        WireProtocol.writeResponse(new DataOutputStream(bytes), expected);
        AccessResponse actual = WireProtocol.readResponse(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(expected, actual);
    }
}

