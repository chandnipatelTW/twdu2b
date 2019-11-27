package com.free2wheelers.models;

import org.assertj.core.util.DateUtil;
import org.junit.Test;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class MessageMetadataTest {

    @Test
    public void shouldReturnStringWithMetadataAndPayload() {
        long millisecondsInUTC = LocalDateTime
                .of(2017, 1, 1, 0, 0)
                .atZone(ZoneId.of("UTC"))
                .toInstant()
                .getEpochSecond() * 1000;

        String producerId = "Producer-id";
        String messageUUID = "123e4567-e89b-12d3-a456-426655440000";
        long size = 12;

        MessageMetadata metadata = new MessageMetadata(millisecondsInUTC, producerId, messageUUID, size);

        String expected = "{\"producer_id\": \"Producer-id\", " +
                "\"size\": 12, " +
                "\"message_id\": \"123e4567-e89b-12d3-a456-426655440000\", " +
                "\"ingestion_time\": 1483228800000}";

        assertEquals(expected, metadata.toString());
    }
}
