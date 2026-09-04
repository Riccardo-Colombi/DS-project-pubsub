package it.polimi.pubsub.utils;

public class ConsoleAppender {

    private static volatile String activePrompt = "";

    public static synchronized void printPrompt(String prompt) {
        activePrompt = prompt;
        System.out.print(activePrompt);
    }

    public static synchronized void asyncMessage(String message) {
        System.out.print("\r" + " ".repeat(80) + "\r");

        System.out.println(message);

        if (!activePrompt.isEmpty()) {
            System.out.print(activePrompt);
        }
    }

    public static synchronized void clearPrompt() {
        activePrompt = "";
    }
}