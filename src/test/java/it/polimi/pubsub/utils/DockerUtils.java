package it.polimi.pubsub.utils;

public class DockerUtils {

    public static void executeCommand(String command) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(command.split(" "));

        builder.inheritIO();

        Process process = builder.start();
        process.waitFor();

        if (process.exitValue() != 0) {
            throw new RuntimeException("Command execution failed: " + command);
        }
    }
}