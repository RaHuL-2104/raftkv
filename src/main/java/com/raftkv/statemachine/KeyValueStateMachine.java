package com.raftkv.statemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KeyValueStateMachine {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);
    private final Map<String, String> store = new ConcurrentHashMap<>();

    public String apply(String command){
        String[] parts = command.split(" ", 3);
        String op = parts[0];

        switch(op){
            case "PUT" -> {
                String key = parts[1];
                String value = parts[2];
                store.put(key, value);
                log.debug("Applied PUT {} = {}", key, value);
                return "OK";
            }
            case "DELETE" -> {
                String key = parts[1];
                store.remove(key);
                log.debug("Applied DELETE {}", key);
                return "OK";
            }
            case "GET" -> {
                String key = parts[1];
                String value = store.get(key);
                return value != null ? value : "(nil)";
            }
            default -> {
                log.warn("Unknown command: {}", command);
                return "ERROR: unknown command";
            }
        }
    }

    public int size(){
        return store.size();
    }

    public Map<String, String> exportState(){
        return new HashMap<>(store);
    }

    public void restoreState(Map<String, String> snapshotData){
        store.clear();
        store.putAll(snapshotData);
    }
}
