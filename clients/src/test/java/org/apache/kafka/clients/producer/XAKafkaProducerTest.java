package org.apache.kafka.clients.producer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class XAKafkaProducerTest {
    @Test
    public void testByteArrayToHex() {
        assertEquals("24af", XAKafkaProducer.byteArrayToHex(new byte[] {(byte) 36, (byte) 175}));
    }
     @Test
    public void testHexToByteArray() {
        assertArrayEquals(new byte[] {(byte) 36, (byte) 175}, XAKafkaProducer.hexToByteArray("24af"));
    }
}
