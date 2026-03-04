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

    public List<Message> recover() throws Exception {
        List<Message> recoveredMessages = new ArrayList<>();

        try(BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while((line = reader.readLine()) != null) {
                if(!line.trim().isEmpty()) {
                    Message msg = serializer.deserialize(line);
                    recoveredMessages.add(msg);
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
}
