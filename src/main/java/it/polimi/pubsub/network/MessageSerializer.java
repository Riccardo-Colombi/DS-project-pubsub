package it.polimi.pubsub.network;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.polimi.pubsub.message.Message;

/**
 * Handles the conversion between Java Message objects and JSON strings for network transmission.
 *
 * - Da client faccio un messaggio e lo serializzo e invio a broker. Poi broker deserializza e ha un oggetto o deve passare gli
 *      attributi deserializzati per il costruttore di nuovo per aver un oggetto Message?
 * - In TcpConnection.receive(), come gestiamo che il broker o anche i client debbano essere costantemente attivi a ricevere il messaggio
 */
public class MessageSerializer {

    private final ObjectMapper mapper;

    public MessageSerializer() {
        this.mapper = new ObjectMapper();
    }

    public String serialize(Message message) throws Exception {
        return mapper.writeValueAsString(message);
    }

    public Message deserialize(String data) throws Exception {
        return mapper.readValue(data, Message.class);
    }
}