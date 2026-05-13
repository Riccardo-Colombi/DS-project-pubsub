package it.polimi.pubsub.network;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.polimi.pubsub.messages.Message;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class TcpConnection {

    private final Socket socket;
    private final MessageSerializer serializer;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public TcpConnection(Socket socket) throws Exception {
        this.socket = socket;
        this.serializer = new MessageSerializer();

        // These are for NDJSON, because a TCP is a Stream, not a package Delivery
        // BufferedReader reads text line-by-line until it sees \n
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // PrintWriter(..., true) enables auto-flushing and handles the \n appending
        this.writer = new PrintWriter(socket.getOutputStream(), true);
    }

    public void send(Message message) throws Exception {
        String jsonPayload = serializer.serialize(message);
        writer.println(jsonPayload);
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
