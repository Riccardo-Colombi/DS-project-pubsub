package it.polimi.pubsub;

import it.polimi.pubsub.broker.Broker;
import it.polimi.pubsub.broker.components.BrokerRole;
import it.polimi.pubsub.client.Client;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Test manuale (nessun assert automatico, si verifica leggendo la console)
 * per i fix fatti sullo snapshot:
 *
 *   B  - trigger size-based
 *   C  - persistenza/ripristino stato Raft (term, commitIndex, lastApplied)
 *   D  - PendingDeliveryMessage: gli ack pendenti sopravvivono correttamente
 *   A+1- nessuna eccezione/corruzione con traffico concorrente durante gli snapshot
 *   2  - i peer del cluster vengono salvati e usati per il rientro automatico
 *
 * Ogni sezione stampa un blocco "=== ... ===" e delle righe "CHECK:" che
 * indicano cosa cercare nell'output per giudicare se il fix corrispondente
 * funziona.
 *
 * NOTA: per la sezione B, adatta NUM_PUBLISH / PAYLOAD_SIZE_BYTES alla soglia
 * di snapshot realmente impostata in Broker.java (per default nel nostro
 * design era 5MB). Se hai impostato una soglia diversa, modifica qui.
 */
public class SnapshotTest {

    private static final String ROOT = "data/snapshotTest";

    public static void main(String[] args) throws Exception {
        cleanDataDirectory(ROOT);
        Thread.sleep(300);

        testSizeBasedTrigger();
        Thread.sleep(1000);

        testRaftMetadataRestore();
        Thread.sleep(1000);

        testPendingDeliveryAfterRestart();
        Thread.sleep(1000);

        testConcurrentTrafficDuringSnapshots();
        Thread.sleep(1000);

        testClusterPeersAutoRejoin();
        Thread.sleep(1000);

        testDistributedSnapshot();

        System.out.println("\n=== Tutti i test lanciati. Controlla i CHECK: sopra. ===");
    }

    // ------------------------------------------------------------------
    // B - Trigger size-based
    // ------------------------------------------------------------------

    // With this test we are checking if the file goes over a certain size (5MB) if the final
    // size is below 5MB
    private static void testSizeBasedTrigger() throws Exception {
        System.out.println("\n=== TEST B: trigger size-based dello snapshot ===");

        String walPath = ROOT + "/B/wal.log";
        String offlinePath = ROOT + "/B/offline";

        Broker broker = new Broker("brokerB", 9190, 9180, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster(); // diventa LEADER da solo, così accetta publish/subscribe

        Client publisher = new Client("pubB", "localhost", 9180);
        publisher.connect();
        Thread.sleep(500);
        publisher.subscribe("topicB"); // così i publish hanno almeno un interessato
        Thread.sleep(300);

        int payloadSize = 20_000; // 20 KB per messaggio
        int numPublish = 400;     // ~8 MB totali di traffico, per superare una soglia da 5MB

        System.out.println("Dimensione WAL PRIMA del traffico: " + walSize(walPath) + " bytes");

        for (int i = 0; i < numPublish; i++) {
            byte[] payload = makePayload(payloadSize);
            publisher.publish("topicB", payload);

            if (i % 50 == 0) {
                Thread.sleep(50); // respiro per non saturare la coda
                System.out.println("  dopo " + i + " publish -> WAL size = " + walSize(walPath) + " bytes");
            }
        }
        Thread.sleep(1500);

        long finalSize = walSize(walPath);
        long theoreticalSizeWithoutSnapshot = (long) numPublish * payloadSize;

        System.out.println("Dimensione WAL DOPO il traffico: " + finalSize + " bytes");
        System.out.println("Dimensione teorica SENZA alcuno snapshot: circa " + theoreticalSizeWithoutSnapshot + " bytes");
        System.out.println("CHECK: se finalSize è nettamente MINORE della dimensione teorica, "
                + "significa che almeno uno snapshot è scattato e ha compattato il WAL. "
                + "Se invece i due valori sono vicini, il trigger size-based NON sta scattando.");

        publisher.disconnect();
        broker.crash();
    }

    private static byte[] makePayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        String chunk = "PAYLOAD-DATA-";
        while (sb.length() < size) sb.append(chunk);
        return sb.substring(0, size).getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------
    // C - Persistenza e ripristino stato Raft
    // ------------------------------------------------------------------

    private static void testRaftMetadataRestore() throws Exception {
        System.out.println("\n=== TEST C: persistenza/ripristino stato Raft ===");

        String walPath = ROOT + "/C/wal.log";
        String offlinePath = ROOT + "/C/offline";

        Broker broker = new Broker("brokerC", 9191, 9181, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster();

        Client client = new Client("clientC", "localhost", 9181);
        client.connect();
        Thread.sleep(300);
        client.subscribeDurable("topicC");
        Thread.sleep(300);

        int payloadSize = 20_000; // 20 KB per messaggio

        int numPublish = 500;
        for (int i = 0; i < numPublish; i++) {
            byte[] payload = makePayload(payloadSize);
        }
        Thread.sleep(4000); // lascia scattare almeno uno snapshot

        int termBeforeCrash = broker.getCurrentTerm();
        int commitIndexBeforeCrash = broker.getCommitIndex();
        int lastAppliedBeforeCrash = broker.getLastApplied();

        System.out.println("PRIMA del crash -> term=" + termBeforeCrash
                + ", commitIndex=" + commitIndexBeforeCrash
                + ", lastApplied=" + lastAppliedBeforeCrash);

        client.disconnect();
        broker.crash();
        Thread.sleep(500);

        // Riavvio VERO: nuova istanza di Broker che legge lo stesso WAL/snapshot da disco
        Broker restarted = new Broker("brokerC", 9191, 9181, walPath, offlinePath);
        restarted.run();
        Thread.sleep(500);

        int termAfterRestart = restarted.getCurrentTerm();
        int commitIndexAfterRestart = restarted.getCommitIndex();
        int lastAppliedAfterRestart = restarted.getLastApplied();

        System.out.println("DOPO il restart -> term=" + termAfterRestart
                + ", commitIndex=" + commitIndexAfterRestart
                + ", lastApplied=" + lastAppliedAfterRestart);

        System.out.println("CHECK: term/commitIndex/lastApplied DOPO il restart devono essere "
                + ">= a quelli PRIMA del crash (idealmente uguali). "
                + "Se dopo il restart term=0 e commitIndex=0, il ripristino dei metadati Raft NON funziona.");

        restarted.crash();
    }

    // ------------------------------------------------------------------
    // D - Ack pendenti corretti dopo snapshot + restart
    // ------------------------------------------------------------------

    private static void testPendingDeliveryAfterRestart() throws Exception {
        System.out.println("\n=== TEST D: correttezza dei pending ack dopo snapshot + restart ===");

        String walPath = ROOT + "/D/wal.log";
        String offlinePath = ROOT + "/D/offline";

        Broker broker = new Broker("brokerD", 9192, 9182, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster();

        Client onlineSub = new Client("d-online", "localhost", 9182);
        Client offlineSub = new Client("d-offline", "localhost", 9182);
        Client publisher = new Client("d-pub", "localhost", 9182);

        onlineSub.connect();
        offlineSub.connect();
        publisher.connect();
        Thread.sleep(300);

        onlineSub.subscribeDurable("topicD");
        offlineSub.subscribeDurable("topicD");
        Thread.sleep(300);

        // d-offline va offline PRIMA della pubblicazione: la sua subscription
        // durevole resta, ma non riceverà il messaggio finché non si riconnette.
        System.out.println("=== d-offline si disconnette (subscription durevole preservata) ===");
        offlineSub.disconnect();
        Thread.sleep(500);

        publisher.publish("topicD", "hello-D".getBytes());
        Thread.sleep(500);
        // A questo punto, guardando la console del BROKER dovresti vedere una riga tipo:
        //   [OfflineStore] Buffering message ... for offline client d-offline
        // e NESSUNA riga di buffering per d-online (che era online e ha ricevuto+ackato subito).

        // Genero traffico extra per far scattare uno snapshot mentre il messaggio
        // per d-offline è ancora "pending" (non buffered solo, ma anche tracciato
        // in AckTracker come atteso da d-offline).

        int payloadSize = 20_000; // 20 KB per messaggio

        for (int i = 0; i < 300; i++) {
            byte[] payload = makePayload(payloadSize);
            publisher.publish("filler-topic", payload);
        }
        Thread.sleep(1500);

        onlineSub.disconnect();
        publisher.disconnect();
        broker.crash();
        Thread.sleep(500);

        System.out.println("=== Riavvio del broker (nuova istanza, stesso WAL) ===");
        Broker restarted = new Broker("brokerD", 9192, 9182, walPath, offlinePath);
        restarted.run();
        Thread.sleep(500);

        System.out.println("=== d-offline si riconnette ===");
        Client reconnectedOfflineSub = new Client("d-offline", "localhost", 9182);
        reconnectedOfflineSub.connect();
        Thread.sleep(1500);

        System.out.println("CHECK 1: nella console del broker riavviato, alla riconnessione di d-offline "
                + "deve comparire ESATTAMENTE UNA riga tipo "
                + "\"[OfflineStore] Delivering 1 buffered messages to d-offline\" "
                + "(non zero -> messaggio perso; non più di una volta -> doppia consegna).");
        System.out.println("CHECK 2: NON deve comparire nessuna consegna/retry per d-online: "
                + "quel client aveva già ackato PRIMA dello snapshot, quindi se il bug D non fosse "
                + "risolto vedresti il broker ritentare l'invio anche verso d-online dopo il restart "
                + "(via retryExpiredMessages, ogni ~2s) perché lo snapshot avrebbe ricalcolato i pending "
                + "usando gli iscritti ATTUALI invece di quelli salvati.");

        reconnectedOfflineSub.disconnect();
        restarted.crash();
    }

    // ------------------------------------------------------------------
    // A + 1 - Traffico concorrente durante gli snapshot
    // ------------------------------------------------------------------

    private static void testConcurrentTrafficDuringSnapshots() throws Exception {
        System.out.println("\n=== TEST A+1: traffico concorrente (connect/disconnect/publish) durante gli snapshot ===");
        System.out.println("CHECK: nessuna eccezione/stack trace deve comparire nella console del broker "
                + "durante questa sezione. Se il lock non copre tutti i path (punto 1), qui è il posto "
                + "più probabile per vedere ConcurrentModificationException o simili.");

        String walPath = ROOT + "/A/wal.log";
        String offlinePath = ROOT + "/A/offline";

        Broker broker = new Broker("brokerA", 9193, 9183, walPath, offlinePath);
        broker.run();
        Thread.sleep(500);
        broker.createCluster();

        int numClients = 6;
        Thread[] threads = new Thread[numClients];

        for (int c = 0; c < numClients; c++) {
            final int id = c;
            threads[c] = new Thread(() -> {
                try {
                    Client client = new Client("conc-client-" + id, "localhost", 9183);
                    client.connect();
                    Thread.sleep(100);

                    int payloadSize = 20_000; // 20 KB per messaggio
                    for (int i = 0; i < 40; i++) {
                        String topic = "topic-" + (i % 3);
                        client.subscribeDurable(topic);
                        byte[] payload = makePayload(payloadSize);
                        client.publish(topic, payload);
                    }

                    Thread.sleep(200); // the client has time to finish the transmission
                    client.disconnect(); // esercita anche il percorso onClientDisconnection
                } catch (Exception e) {
                    System.err.println("ERRORE nel client concorrente " + id + ": " + e);
                    e.printStackTrace();
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        Thread.sleep(1000);
        System.out.println("Traffico concorrente completato senza eccezioni non gestite (verifica sopra).");

        broker.crash();
    }

    // ------------------------------------------------------------------
    // 2 - Peer del cluster salvati + rientro automatico
    // ------------------------------------------------------------------

    private static void testClusterPeersAutoRejoin() throws Exception {
        System.out.println("\n=== TEST 2: peer del cluster salvati nello snapshot + rientro automatico ===");

        String leaderWal = ROOT + "/CLUSTER/wal_leader.log";
        String leaderOffline = ROOT + "/CLUSTER/offline_leader";
        String followerWal = ROOT + "/CLUSTER/wal_follower.log";
        String followerOffline = ROOT + "/CLUSTER/offline_follower";

        Broker leader = new Broker("leadCP", 9194, 9184, leaderWal, leaderOffline);
        leader.run();
        Thread.sleep(300);
        leader.createCluster();

        Broker follower = new Broker("follCP", 9195, 9185, followerWal, followerOffline);
        follower.run();
        Thread.sleep(300);
        follower.joinCluster("localhost", 9194);
        Thread.sleep(800);

        System.out.println("Ruolo follower dopo il join: " + follower.getRole());

        // Genero traffico per far scattare uno snapshot sul FOLLOWER, che deve
        // salvarsi il peer 'leadCP' tra i suoi metadati Raft.
        Client client = new Client("cpClient", "localhost", 9184);
        client.connect();
        Thread.sleep(300);
        int payloadSize = 20_000; // 20 KB per messaggio
        for (int i = 0; i < 300; i++) {
            byte[] payload = makePayload(payloadSize);
            client.publish("cp-topic", payload);
        }
        Thread.sleep(4000);
        client.disconnect();

        System.out.println("=== Crasho SOLO il follower (il leader resta up) ===");
        follower.crash();
        Thread.sleep(500);

        System.out.println("=== Riavvio il follower SENZA chiamare joinCluster() manualmente ===");
        Broker restartedFollower = new Broker("follCP", 9195, 9185, followerWal, followerOffline);
        restartedFollower.run(); // <-- NIENTE joinCluster qui: deve rientrare da solo

        long deadline = System.currentTimeMillis() + 10_000;
        BrokerRole roleSeen = restartedFollower.getRole();
        while (System.currentTimeMillis() < deadline && roleSeen == BrokerRole.SOLO) {
            Thread.sleep(200);
            roleSeen = restartedFollower.getRole();
        }

        System.out.println("Ruolo del follower riavviato (entro 10s dal restart): " + roleSeen);
        System.out.println("CHECK: il ruolo NON deve essere rimasto SOLO. Se lo è, i peer non sono stati "
                + "salvati nello snapshot oppure il rientro automatico allo startup non è scattato "
                + "(vedi Broker.run() -> attemptClusterRejoin()).");

        restartedFollower.crash();
        leader.crash();
    }

    // ------------------------------------------------------------------
    // E - Distributed Snapshot
    // ------------------------------------------------------------------

    private static void testDistributedSnapshot() throws Exception {
        System.out.println("\n=== TEST E: Distributed Snapshot (InstallSnapshot RPC) ===");

        String leaderWal = ROOT + "/DSNAP/wal_leader.log";
        String leaderOffline = ROOT + "/DSNAP/offline_leader";
        String followerWal = ROOT + "/DSNAP/wal_follower.log";
        String followerOffline = ROOT + "/DSNAP/offline_follower";

        Broker leader = new Broker("leadDS", 9196, 9186, leaderWal, leaderOffline);
        leader.run();
        Thread.sleep(300);
        leader.createCluster();

        Broker follower = new Broker("follDS", 9197, 9187, followerWal, followerOffline);
        follower.run();
        Thread.sleep(300);
        follower.joinCluster("localhost", 9196);
        Thread.sleep(800);

        System.out.println("=== Disconnecting Follower ===");
        follower.crash();
        Thread.sleep(500);

        System.out.println("=== Pushing heavy traffic to Leader to trigger local snapshot ===");
        Client publisher = new Client("pubDS", "localhost", 9186);
        publisher.connect();
        Thread.sleep(300);
        publisher.subscribeDurable("topicDS");

        int payloadSize = 20_000;
        int numPublish = 500;

        for (int i = 0; i < numPublish; i++) {
            publisher.publish("topicDS", makePayload(payloadSize));
        }
        Thread.sleep(4000);
        publisher.disconnect();

        int leaderCommitIndex = leader.getCommitIndex();
        System.out.println("Leader commitIndex after log compaction: " + leaderCommitIndex);

        System.out.println("=== Restarting Follower ===");
        Broker restartedFollower = new Broker("follDS", 9197, 9187, followerWal, followerOffline);
        restartedFollower.run();

        restartedFollower.joinCluster("localhost", 9196);
        Thread.sleep(800);

        // Wait for the leader to send the InstallSnapshotMessage and the follower to install it
        Thread.sleep(4000);

        int followerCommitIndex = restartedFollower.getCommitIndex();
        System.out.println("Follower commitIndex after synchronization: " + followerCommitIndex);

        System.out.println("CHECK: The Follower's commitIndex should match the Leader's commitIndex (" + leaderCommitIndex + "), " +
                "meaning it successfully installed the state via network.");

        restartedFollower.crash();
        leader.crash();
    }

    // ------------------------------------------------------------------
    // Utility
    // ------------------------------------------------------------------

    private static long walSize(String path) {
        try {
            return Files.size(Paths.get(path));
        } catch (IOException e) {
            return -1; // file non ancora creato o già ruotato
        }
    }

    private static void cleanDataDirectory(String dirPath) throws IOException {
        Path root = Paths.get(dirPath);
        if (!Files.exists(root)) {
            Files.createDirectories(root);
            System.out.println("Data directory creata (mancava): " + root.toAbsolutePath());
            return;
        }

        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) throw exc;
                if (!dir.equals(root)) {
                    Files.delete(dir);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        System.out.println("Data directory pulita: " + root.toAbsolutePath());
    }
}