package com.raftkv.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WriteAheadLogTest {

    private static final String TEST_NODE_ID = "test-node-wal";

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
    void replay_onFreshWal_returnsEmptyList() throws IOException {
        WriteAheadLog wal = new WriteAheadLog(TEST_NODE_ID);
        wal.open();

        List<WalRecord> records = wal.replay();

        assertTrue(records.isEmpty());
        wal.close();
    }

    @Test
    void append_thenReplay_recoversAllRecordsInOrder() throws IOException {
        WriteAheadLog wal = new WriteAheadLog(TEST_NODE_ID);
        wal.open();

        wal.append(WalRecord.forTerm(1));
        wal.append(WalRecord.forVote(1, "node-A"));
        wal.append(WalRecord.forEntry(1, 1, "PUT foo bar"));
        wal.close();

        // Reopen fresh, as if the process restarted
        WriteAheadLog walAfterRestart = new WriteAheadLog(TEST_NODE_ID);
        walAfterRestart.open();
        List<WalRecord> records = walAfterRestart.replay();

        assertEquals(3, records.size());
        assertEquals(WalRecord.Type.TERM, records.get(0).getType());
        assertEquals(1, records.get(0).getTerm());
        assertEquals(WalRecord.Type.VOTE, records.get(1).getType());
        assertEquals("node-A", records.get(1).getVotedFor());
        assertEquals(WalRecord.Type.ENTRY, records.get(2).getType());
        assertEquals("PUT foo bar", records.get(2).getCommand());

        walAfterRestart.close();
    }

    @Test
    void replay_withCorruptFinalLine_recoversEverythingBeforeIt() throws IOException {
        WriteAheadLog wal = new WriteAheadLog(TEST_NODE_ID);
        wal.open();
        wal.append(WalRecord.forTerm(1));
        wal.append(WalRecord.forEntry(1, 1, "PUT a 1"));
        wal.close();

        // Simulate a crash mid-write: append a half-written, invalid JSON
        // line directly to the file, bypassing the normal append() method.
        Path walPath = Path.of("data", TEST_NODE_ID, "wal.log");
        Files.write(walPath, "{\"type\":\"ENTRY\",\"term\":1,\"index\":2,\"comm".getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.APPEND);

        WriteAheadLog walAfterCrash = new WriteAheadLog(TEST_NODE_ID);
        walAfterCrash.open();
        List<WalRecord> records = walAfterCrash.replay();

        // The two valid records before the corrupt line must still be recovered
        assertEquals(2, records.size(), "valid records before a corrupt line must still be recovered");
        assertEquals(WalRecord.Type.TERM, records.get(0).getType());
        assertEquals(WalRecord.Type.ENTRY, records.get(1).getType());

        walAfterCrash.close();
    }

    @Test
    void truncate_removesAllExistingRecords() throws IOException {
        WriteAheadLog wal = new WriteAheadLog(TEST_NODE_ID);
        wal.open();
        wal.append(WalRecord.forTerm(1));
        wal.append(WalRecord.forEntry(1, 1, "PUT a 1"));

        wal.truncate();

        List<WalRecord> records = wal.replay();
        assertTrue(records.isEmpty(), "truncate() should leave no records behind");
        wal.close();
    }
}
