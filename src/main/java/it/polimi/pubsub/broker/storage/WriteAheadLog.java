package it.polimi.pubsub.broker.storage;

import it.polimi.pubsub.messages.Message;
import it.polimi.pubsub.network.MessageSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 * - How to empty the WAL?
 *      Checkpointing or snapshots: instead of emptying the WAL operation per operation, you stop accepting new
 *      messages and create a snapshot by tanking the TopicSubscribersTable and AckTracker and put in a snapshot.json file.
 *      Then rename it in wal.json.
 * - When to do it?
 *      Sized based: do this every time the file wal.json exceed 10MB in size.
 *      Count based: do this every time the broker appends M messages to the wal.json.
 *
 *  Note: how to clean wal.json file during startup: java -jar pubsub.jar broker --clean
 */

// I decided to create a WAL file to save the state of a broker and append messages everytime instead of modifying it
// every time there's a "new thing" or there is a "deletion operation".

    // TODO Update this to be coherent with Raft

public class WriteAheadLog {

    private final File logFile;
    private final MessageSerializer serializer;
    private BufferedWriter writer;

    public WriteAheadLog(String filePath) throws IOException {
        this.logFile = new File(filePath);
        this.serializer = new MessageSerializer();

        if(!logFile.exists()) {
            logFile.getParentFile().mkdirs();
            logFile.createNewFile();
        }
        this.writer = new BufferedWriter(new FileWriter(logFile, true));
    }

    public synchronized void append(Message message) throws Exception {
        String jsonPayload = serializer.serialize(message);
        writer.write(jsonPayload);
        writer.newLine();

        // to put it in permanent memory
        writer.flush();
    }

    // TODO Modify this method for snapshot implementation
    // Not necessary

    public List<Message> recover() throws Exception {
        List<Message> recoveredMessages = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while((line = reader.readLine()) != null) {
                String trimmedLine = line.trim();
                if(!trimmedLine.isEmpty()) {
                    try {
                        // If line is corrupted, e.g. crash while writing, serializer should throw an exception
                        // TODO it really throws an exception???
                        Message msg = serializer.deserialize(trimmedLine);
                        recoveredMessages.add(msg);
                    } catch (Exception e) {
                        // Do nothing, it means the message was not acknowledged and it will be resent
                        System.err.println("Corrupted line ... skipping : " + trimmedLine);
                    }
                }
            }
        }
        return recoveredMessages;
    }

    public synchronized void close() throws IOException {
        if(writer != null) {
            writer.close();
        }
    }

    // TODO Snapshot managing methods
    // they are called by the broker when the conditions are met

    // TODO Distributed Snapshot

    public synchronized void snapshot(List<Message> consolidatedState) throws Exception {
        File tempFile = new File(logFile.getAbsolutePath() + ".tmp");

        try (BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile))) {
            for (Message msg : consolidatedState) {
                tempWriter.write(serializer.serialize(msg));
                tempWriter.newLine();
            }
            tempWriter.flush();
        } catch (Exception e) {
            if (tempFile.exists()) tempFile.delete();
            throw e;
        }

        this.close();
        boolean success = tempFile.renameTo(logFile);

        try {
            this.writer = new BufferedWriter(new FileWriter(logFile, true));
        } catch (IOException e) {
            throw new IOException("fatal error, not able to open wal", e);
        }

        if (!success) {
            if (tempFile.exists()) tempFile.delete();
            throw new IOException("failed snapshot, keep using old WAL");
        }
    }

    // To check dimensions of the WAL
    public long getSize() {
        return logFile.length();
    }

}
