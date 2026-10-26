package com.featureforge.testkit;

import com.featureforge.proto.EventType;
import com.featureforge.proto.RetailEvent;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;

public final class DeterministicEventLoader {

    public record RawRow(long timestampMs, long visitorId, EventType type, long itemId, String transactionId)
            implements Comparable<RawRow> {
        @Override
        public int compareTo(RawRow o) {
            int cmp = Long.compare(this.visitorId, o.visitorId);
            if (cmp != 0) return cmp;
            cmp = Long.compare(this.itemId, o.itemId);
            if (cmp != 0) return cmp;
            cmp = Long.compare(this.timestampMs, o.timestampMs);
            if (cmp != 0) return cmp;
            return Integer.compare(this.type.getNumber(), o.type.getNumber());
        }
    }

    public static List<RetailEvent> loadAndInjectDelays(Path csvPath, long seed) throws Exception {
        List<RawRow> rows = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null || !header.equals("timestamp,visitorid,event,itemid,transactionid")) {
                throw new IllegalArgumentException("Day-1 Gate Failed: Unexpected CSV header: " + header);
            }
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", -1);
                long ts = Long.parseLong(parts[0]);
                long visitor = Long.parseLong(parts[1]);
                EventType type = switch (parts[2]) {
                    case "view" -> EventType.VIEW;
                    case "addtocart" -> EventType.ADD_TO_CART;
                    case "transaction" -> EventType.TRANSACTION;
                    default -> throw new IllegalArgumentException("Unknown event type: " + parts[2]);
                };
                long item = Long.parseLong(parts[3]);
                String txId = parts.length > 4 ? parts[4] : "";
                rows.add(new RawRow(ts, visitor, type, item, txId));
            }
        }

        // Single-threaded deterministic sort to assign occurrence index 
        Collections.sort(rows);

        List<RetailEvent> baseEvents = new ArrayList<>(rows.size());
        RawRow prev = null;
        int occurrenceIndex = 0;

        for (RawRow r : rows) {
            if (prev != null && prev.compareTo(r) == 0) {
                occurrenceIndex++;
            } else {
                occurrenceIndex = 0;
            }
            prev = r;
            String eventId = sha256Hex(r.visitorId + ":" + r.itemId + ":" + r.timestampMs + ":" + r.type.name() + ":" + occurrenceIndex);
            baseEvents.add(RetailEvent.newBuilder()
                    .setEventId(eventId)
                    .setVisitorId(r.visitorId)
                    .setItemId(r.itemId)
                    .setEventType(r.type)
                    .setEventTimeMs(r.timestampMs)
                    .setAvailableAtMs(r.timestampMs)
                    .setTransactionId(r.transactionId)
                    .setSchemaVersion(1)
                    .build());
        }

        // Sort chronologically by event_time_ms before injecting seeded network delays
        baseEvents.sort(Comparator.comparingLong(RetailEvent::getEventTimeMs));

        Random rng = new Random(seed);
        List<RetailEvent> injectedStream = new ArrayList<>((int) (baseEvents.size() * 1.02));

        for (RetailEvent ev : baseEvents) {
            double roll = rng.nextDouble();
            long delayMs;
            if (roll < 0.94) {
                delayMs = 0L; // 94% on time
            } else if (roll < 0.99) {
                delayMs = (long) (rng.nextDouble() * 10 * 60_000L); // 5% uniform 0 to 10 mins
            } else {
                delayMs = (long) ((10 + rng.nextDouble() * 110) * 60_000L); // 1% uniform 10m to 2h
            }

            RetailEvent withDelay = ev.toBuilder()
                    .setAvailableAtMs(ev.getEventTimeMs() + delayMs)
                    .build();
            injectedStream.add(withDelay);

            // 1% duplicate delivery with identical event_id
            if (rng.nextDouble() < 0.01) {
                injectedStream.add(withDelay);
            }
        }

        // Order stream by available_at_ms (arrival order at ingest gateway)
        injectedStream.sort(Comparator.comparingLong(RetailEvent::getAvailableAtMs));
        return injectedStream;
    }

    private static String sha256Hex(String input) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest).substring(0, 24);
    }
}