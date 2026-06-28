package com.raftkv.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotStoreTest {

    private static final String TEST_NODE_ID = "test-node-snapshot";

    @AfterEach
    void tearDown() throws IOException {
        Path dir = Path.of("data", TEST_NODE_ID);
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a))
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
    }

    @Test
    void load_whenNoSnapshotExists_returnsEmpty() throws IOException {
        SnapshotStore store = new SnapshotStore(TEST_NODE_ID);
        Optional<Snapshot> result = store.load();
        assertTrue(result.isEmpty());
    }

    @Test
    void save_thenLoad_roundTripsAllFieldsCorrectly() throws IOException {
        SnapshotStore store = new SnapshotStore(TEST_NODE_ID);
        Snapshot original = new Snapshot(42L, 7L, Map.of("a", "1", "b", "2"));

        store.save(original);
        Optional<Snapshot> loaded = store.load();

        assertTrue(loaded.isPresent());
        assertEquals(42L, loaded.get().getLastIncludedIndex());
        assertEquals(7L, loaded.get().getLastIncludedTerm());
        assertEquals(Map.of("a", "1", "b", "2"), loaded.get().getStateMachineData());
    }

    @Test
    void save_calledTwice_secondSnapshotFullyReplacesTheFirst() throws IOException {
        SnapshotStore store = new SnapshotStore(TEST_NODE_ID);

        store.save(new Snapshot(5L, 1L, Map.of("old", "data")));
        store.save(new Snapshot(10L, 2L, Map.of("new", "data")));

        Optional<Snapshot> loaded = store.load();
        assertTrue(loaded.isPresent());
        assertEquals(10L, loaded.get().getLastIncludedIndex());
        assertEquals(Map.of("new", "data"), loaded.get().getStateMachineData(),
                "second save should fully replace the first, not merge with it");
    }

    @Test
    void save_doesNotLeaveTempFileBehind() throws IOException {
        SnapshotStore store = new SnapshotStore(TEST_NODE_ID);
        store.save(new Snapshot(1L, 1L, Map.of("a", "1")));

        Path tempPath = Path.of("data", TEST_NODE_ID, "snapshot.json.tmp");
        assertFalse(Files.exists(tempPath), "atomic rename should leave no .tmp file behind");
    }
}