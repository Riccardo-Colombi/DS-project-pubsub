package it.polimi.pubsub.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.polimi.pubsub.message.Message;

/**
 * Handles the conversion between Java Message objects and JSON byte arrays for network transmission.
 */
public class MessageSerializer {

    private final ObjectMapper mapper;

    public MessageSerializer() {
        this.mapper = new ObjectMapper();
    }

    public byte[] serialize(Message message) throws Exception {
        return mapper.writeValueAsBytes(message);
    }

    public Message deserialize(byte[] data) throws Exception {
        return mapper.readValue(data, Message.class);
    }
}