package it.polimi.pubsub.broker.storage;

import it.polimi.pubsub.messages.Message;

public class LogEntry {

    private final int term;
    private final Message message;

    public LogEntry(int term, Message message) {
        this.term = term;
        this.message = message;
    }

    public int getTerm() {
        return term;
    }

    public Message getCommand() {
        return message;
    }

}
