package com.raftkv.statemachine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStateMachineTest {

    private KeyValueStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new KeyValueStateMachine();
    }

    @Test
    void put_storesValueAndReturnsOk() {
        String result = stateMachine.apply("PUT foo bar");
        assertEquals("OK", result);
    }

    @Test
    void get_afterPut_returnsTheStoredValue() {
        stateMachine.apply("PUT foo bar");
        String result = stateMachine.apply("GET foo");
        assertEquals("bar", result);
    }

    @Test
    void get_forMissingKey_returnsNilMarker() {
        String result = stateMachine.apply("GET doesNotExist");
        assertEquals("(nil)", result);
    }

    @Test
    void delete_removesTheKey() {
        stateMachine.apply("PUT foo bar");
        stateMachine.apply("DELETE foo");
        String result = stateMachine.apply("GET foo");
        assertEquals("(nil)", result);
    }

    @Test
    void put_withValueContainingSpaces_storesEntireRemainderAsValue() {
        // Command.split(" ", 3) means the value can itself contain spaces —
        // important since values aren't restricted to single words.
        stateMachine.apply("PUT greeting hello world how are you");
        String result = stateMachine.apply("GET greeting");
        assertEquals("hello world how are you", result);
    }

    @Test
    void unknownCommand_returnsErrorWithoutThrowing() {
        String result = stateMachine.apply("FROBNICATE foo");
        assertEquals("ERROR: unknown command", result);
    }

    @Test
    void exportState_returnsDefensiveCopyNotLiveReference() {
        stateMachine.apply("PUT a 1");
        Map<String, String> exported = stateMachine.exportState();

        // Mutating the exported copy must NOT affect the real store
        exported.put("a", "TAMPERED");

        assertEquals("1", stateMachine.apply("GET a"), "exportState must return a copy, not a live reference");
    }

    @Test
    void restoreState_replacesEntireStoreContents() {
        stateMachine.apply("PUT old value");

        stateMachine.restoreState(Map.of("new", "data"));

        assertEquals("(nil)", stateMachine.apply("GET old"), "old keys should be gone after restore");
        assertEquals("data", stateMachine.apply("GET new"));
    }

    @Test
    void size_reflectsNumberOfStoredKeys() {
        stateMachine.apply("PUT a 1");
        stateMachine.apply("PUT b 2");
        stateMachine.apply("PUT c 3");
        assertEquals(3, stateMachine.size());

        stateMachine.apply("DELETE b");
        assertEquals(2, stateMachine.size());
    }
}

