package com.raftkv;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.raftkv.config.NodeConfig;
import com.raftkv.config.PeerConfig;
import com.raftkv.raft.NotLeaderException;
import com.raftkv.raft.RaftNode;


/**
 * Hello world!
 *
 */
public final class App 
{
    public static void main( String[] args ) throws InterruptedException
    {
        // System.out.println( "Hello World!" );
        if(args.length < 2){
            System.err.println("Usage: App <nodeId> <port> [peerId:host:port ...]");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);

        List<PeerConfig> peers = new ArrayList<>();
        for(int i = 2; i < args.length; i++){
            String[] parts = args[i].split(":");
            peers.add(new PeerConfig(parts[0], parts[1], Integer.parseInt(parts[2])));
        }

        NodeConfig config = new NodeConfig(nodeId, "localhost", port, peers, 150, 300, 50);

        RaftNode node = new RaftNode(config);

        Runtime.getRuntime().addShutdownHook(new Thread(node::shutdown));
        node.start();

        // while(true){
        //     System.out.printf("[%s] role=%s term=%d leader=%s%n", node.getNodeId(), node.getRole(), node.getCurrentTerm(), node.getCurrentLeaderId());
        //     Thread.sleep(1000);
        // }

        Thread statusPrinter = new Thread(() -> {
            while(true){
                try{
                    System.out.printf("[%s] role=%s term=%d leader=%s%n",
                        node.getNodeId(), node.getRole(), node.getCurrentTerm(), node.getCurrentLeaderId()
                    );
                    Thread.sleep(2000);
                } catch(InterruptedException e){
                    return;
                }
            }
        });
        statusPrinter.setDaemon(true);
        statusPrinter.start();

        System.out.println("READYY !! Type commands like PUT foo bar | GET foo | DELETE foo");
        Scanner sc = new Scanner(System.in);
        while(sc.hasNextLine()){
            String line = sc.nextLine().trim();
            if(line.isEmpty()) continue;

            try{
                String result = node.submitCommand(line).get(2, TimeUnit.SECONDS);
                System.out.println(">> " + result);
            } catch(NotLeaderException e){
                System.out.println(">> NOT LEADER. Try node: " + e.getKnownLeaderId());
            } catch(ExecutionException e){
                if(e.getCause() instanceof NotLeaderException nle){
                    System.out.println(">> NOT LEADER. Try node: " + nle.getKnownLeaderId());
                }else{
                    System.out.println(">> ERROR: " + e.getCause());
                }
            } catch(TimeoutException e){
                System.out.println(">> TIMEOUT - command may not have committed");
            }
        }
        sc.close();
    }
}
