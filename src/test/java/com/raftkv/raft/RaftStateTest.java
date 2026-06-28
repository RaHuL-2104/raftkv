package com.raftkv.raft;

import com.raftkv.storage.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftStateTest {

    private static final String TEST_NODE_ID = "test-node-raftstate";
    private WriteAheadLog wal;
    private RaftState state;

    @BeforeEach
    void setUp() throws IOException {
        // Each test gets a fresh WAL backed by a real (but disposable) file
        // under data/test-node-raftstate — deleted in tearDown.
        wal = new WriteAheadLog(TEST_NODE_ID);
        wal.open();
        state = new RaftState(wal);
    }

    @AfterEach
    void tearDown() throws IOException {
        wal.close();
        Path dir = Path.of("data", TEST_NODE_ID);
        if (Files.exists(dir)) {
            Files.walk(dir)
                    .sorted((a, b) -> b.compareTo(a)) // delete files before their parent dir
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        }
    }

    @Test
    void newState_startsAsFollowerWithTermZero() {
        assertEquals(0, state.getCurrentTerm());
        assertEquals(RaftState.Role.FOLLOWER, state.getRole());
        assertNull(state.getVotedFor());
        assertEquals(0, state.getLastLogIndex());
        assertEquals(0, state.getLastLogTerm());
    }

    @Test
    void incrementTerm_increasesByOneEachCall() {
        assertEquals(1, state.incrementTerm());
        assertEquals(2, state.incrementTerm());
        assertEquals(3, state.incrementTerm());
        assertEquals(3, state.getCurrentTerm());
    }

    @Test
    void setVotedFor_persistsAndIsRetrievable() {
        state.setVotedFor("node-7");
        assertEquals("node-7", state.getVotedFor());
    }

    @Test
    void appendEntry_updatesLastLogIndexAndTerm() {
        state.appendEntry(new LogEntry(1, 1, "PUT a 1"));
        state.appendEntry(new LogEntry(1, 2, "PUT b 2"));

        assertEquals(2, state.getLastLogIndex());
        assertEquals(1, state.getLastLogTerm());
        assertEquals(2, state.getLogSize());
    }

    @Test
    void getEntry_retrievesCorrectEntryByOneBasedLogIndex() {
        state.appendEntry(new LogEntry(1, 1, "PUT a 1"));
        state.appendEntry(new LogEntry(2, 2, "PUT b 2"));

        assertEquals("PUT a 1", state.getEntry(1).getCommand());
        assertEquals("PUT b 2", state.getEntry(2).getCommand());
    }

    @Test
    void appendOrOverwrite_appendsNewEntriesWhenNoConflict() {
        state.appendEntry(new LogEntry(1, 1, "PUT a 1"));

        state.appendOrOverwrite(List.of(
                new LogEntry(1, 2, "PUT b 2"),
                new LogEntry(1, 3, "PUT c 3")
        ));

        assertEquals(3, state.getLastLogIndex());
        assertEquals("PUT b 2", state.getEntry(2).getCommand());
        assertEquals("PUT c 3", state.getEntry(3).getCommand());
    }

    @Test
    void appendOrOverwrite_truncatesConflictingEntriesAndTakesNewVersion() {
        // Simulate a follower that accepted entries from an old leader
        state.appendEntry(new LogEntry(1, 1, "PUT a 1"));
        state.appendEntry(new LogEntry(1, 2, "PUT WRONG 99")); // will conflict
        state.appendEntry(new LogEntry(1, 3, "PUT WRONG 100")); // will be truncated away

        // New leader (higher term) sends the correct entry for index 2 onward
        state.appendOrOverwrite(List.of(
                new LogEntry(2, 2, "PUT b 2")
        ));

        assertEquals(2, state.getLastLogIndex(), "conflicting entry 3 should have been truncated");
        assertEquals("PUT b 2", state.getEntry(2).getCommand());
        assertEquals(2, state.getEntry(2).getTerm());
    }

    @Test
    void restoreFromSnapshot_setsOffsetAndAppliedIndexesCorrectly() {
        state.restoreFromSnapshot(10, 4);

        assertEquals(10, state.getSnapshotOffset());
        assertEquals(4, state.getSnapshotTerm());
        assertEquals(10, state.getLastApplied());
        assertEquals(10, state.getCommitIndex());
        // No entries appended yet — lastLogIndex/Term should reflect the snapshot
        assertEquals(10, state.getLastLogIndex());
        assertEquals(4, state.getLastLogTerm());
    }

    @Test
    void getEntry_afterSnapshot_correctlyAccountsForOffset() {
        state.restoreFromSnapshot(10, 4);
        state.appendEntry(new LogEntry(5, 11, "PUT x 1"));
        state.appendEntry(new LogEntry(5, 12, "PUT y 2"));

        assertEquals("PUT x 1", state.getEntry(11).getCommand());
        assertEquals("PUT y 2", state.getEntry(12).getCommand());
        assertEquals(12, state.getLastLogIndex());
    }

    @Test
    void compactLogUpTo_removesOnlyEntriesUpToGivenIndex() {
        state.appendEntry(new LogEntry(1, 1, "PUT a 1"));
        state.appendEntry(new LogEntry(1, 2, "PUT b 2"));
        state.appendEntry(new LogEntry(1, 3, "PUT c 3"));

        state.compactLogUpTo(2, 1);

        assertEquals(2, state.getSnapshotOffset());
        // Only entry at index 3 should remain in memory
        assertEquals("PUT c 3", state.getEntry(3).getCommand());
        assertEquals(3, state.getLastLogIndex());
    }
}