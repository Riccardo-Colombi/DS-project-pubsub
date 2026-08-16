package it.polimi.pubsub.network;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.Message;

import java.io.*;
import java.net.Socket;

public class TcpConnection {

    private final Socket socket;
    private final MessageSerializer serializer;
    private final BufferedReader reader;
    private final BufferedWriter writer;

    public TcpConnection(Socket socket) throws Exception {
        this.socket = socket;
        this.serializer = new MessageSerializer();

        // These are for NDJSON, because a TCP is a Stream, not a package Delivery
        // BufferedReader reads text line-by-line until it sees \n
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // PrintWriter(..., true) enables auto-flushing and handles the \n appending
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    // Added synchronized because in case of big messages, a T1 must finish sending the whole stream
    // before another T2 start sending his stream.
    public synchronized void send(Message message) throws Exception {
        String jsonPayload = serializer.serialize(message);
        writer.write(jsonPayload);

        writer.newLine();
        writer.flush(); // Push the data through the socket immediately
    }

    public Message receive() throws IOException, JsonProcessingException {
        String jsonPayload = reader.readLine();

        // Socket closed by the other side
        if (jsonPayload == null) {
            return null;
        }

        return serializer.deserialize(jsonPayload);
    }

    public void close() throws IOException {
        socket.close();
    }

    public String getRemoteHost() {
        return socket.getInetAddress().getHostAddress();
    }

}
