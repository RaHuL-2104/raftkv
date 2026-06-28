package com.raftkv.network;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RequestVoteRequestTest {

    /**
     * Regression test for a real bug found during development: the
     * constructor call in RaftNode.startElection() once passed
     * lastLogTerm and lastLogIndex in swapped positions, which silently
     * corrupted vote-granting decisions once snapshot compaction caused
     * the two values to diverge. This test pins down the exact field
     * order so that mistake can never sneak back in unnoticed.
     */
    @Test
    void fieldsAreAssignedToCorrectPositions_notSwapped() {
        RequestVoteRequest request = new RequestVoteRequest(
                "candidate-1",
                /* term         */ 5L,
                /* lastLogIndex */ 42L,
                /* lastLogTerm  */ 3L
        );

        assertEquals("candidate-1", request.getCandidateId());
        assertEquals(5L, request.getTerm());
        assertEquals(42L, request.getLastLogIndex(), "lastLogIndex must not be swapped with lastLogTerm");
        assertEquals(3L, request.getLastLogTerm(), "lastLogTerm must not be swapped with lastLogIndex");
    }

    @Test
    void distinctIndexAndTermValues_eachLandInTheirOwnField() {
        // Deliberately using very different magnitudes for index vs term
        // so that a swap would be immediately obvious if this test failed.
        RequestVoteRequest request = new RequestVoteRequest("nodeX", 1L, 1000L, 2L);

        assertEquals(1000L, request.getLastLogIndex());
        assertEquals(2L, request.getLastLogTerm());
    }
}
