package com.raftkv.storage;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class WriteAheadLog {
    private static final Logger log = LoggerFactory.getLogger(WriteAheadLog.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Path filePath;
    private RandomAccessFile file;
    private FileOutputStream outputStream;

    public WriteAheadLog(String nodeId){
        this.filePath = Paths.get("data", nodeId, "wal.log");
    }

    public void open() throws IOException{
        Files.createDirectories(filePath.getParent());
        outputStream = new FileOutputStream(filePath.toFile(), true);
        log.info("WAL opened at {}", filePath.toAbsolutePath());
    }

    public synchronized void append(WalRecord record) throws IOException{
        String json = MAPPER.writeValueAsString(record);
        outputStream.write((json + "\n").getBytes(StandardCharsets.UTF_8));
        outputStream.flush();
        outputStream.getFD().sync();
    }

    public List<WalRecord> replay() throws IOException{
        List<WalRecord> records = new ArrayList<>();
        if(!Files.exists(filePath)){
            log.info("No existing WAL found at {} - starting fresh", filePath.toAbsolutePath());
            return records;
        }

        try(BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)){
            String line;
            int lineNumber = 0;
            while((line = reader.readLine()) != null){
                lineNumber++;
                if(line.isBlank()) continue;
                try{
                    records.add(MAPPER.readValue(line, WalRecord.class));
                } catch(Exception e){
                    log.warn("WAL line {} unreadable (likely a partial write from a crash) - " + "stopping replay here, {} records recovered", lineNumber, records.size());
                    break;
                }
            }
        }
        log.info("WAL replay recovered {} records from {}", records.size(), filePath.toAbsolutePath());
        return records;
    }

    public synchronized void truncate() throws IOException{
        close();
        Files.deleteIfExists(filePath);
        open();
        log.info("WAL truncated at {}", filePath.toAbsolutePath());
    }

    public synchronized void close() throws IOException{
        if(outputStream != null){
            outputStream.close();
        }
    }
}
