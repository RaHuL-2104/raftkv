package com.raftkv.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class SnapshotStore {
    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path snapshotPath;
    private final Path tempPath;

    public SnapshotStore(String nodeId){
        this.snapshotPath = Paths.get("data", nodeId, "snapshot.json");
        this.tempPath = Paths.get("data", nodeId, "snapshot.json.tmp");
    }

    public void save(Snapshot snapshot) throws IOException{
        Files.createDirectories(snapshotPath.getParent());

        byte[] json = MAPPER.writeValueAsBytes(snapshot);
        Files.write(tempPath, json);

        try(var channel = java.nio.channels.FileChannel.open(tempPath, java.nio.file.StandardOpenOption.WRITE)){
            channel.force(true);
        }

        Files.move(tempPath, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

        log.info("Snapshot saved: lastIncludedIndex={}, lastIncludedTerm={}, keys={}", snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(), snapshot.getStateMachineData().size());
    }

    public Optional<Snapshot> load() throws IOException{
        if(!Files.exists(snapshotPath)){
            return Optional.empty();
        }

        Snapshot snapshot = MAPPER.readValue(snapshotPath.toFile(), Snapshot.class);
        log.info("Loaded snapshot: lastIncludedIndex={}, lastIncludedTerm={}, keys={}", snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(), snapshot.getStateMachineData().size());
        return Optional.of(snapshot);
    }
}
