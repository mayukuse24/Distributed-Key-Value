package app;

import java.net.*;
import java.security.InvalidParameterException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.time.*;


public class Client extends Node {
    public List<Node> serverList = new ArrayList<Node>();

    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());

    public Client(String Id) {
        super(Id);
    }

    public void loadConfig(String fileName) {
        try {
            BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
            String line;
            String[] params;

            LOGGER.info("loading servers from config file");

            while ((line = inputBuffer.readLine()) != null) {
                params = line.split(" ");

                LOGGER.info(String.format("Found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }

            inputBuffer.close();
        }
        catch (Exception e) {
            System.out.println(String.format("Could not load config from file: %s", fileName));
        }  
    }

    public static void main(String[] args) throws Exception {
        PrintWriter writer;
        BufferedReader reader;
        Socket reqSocket = null;
        Node selectedServer;
        String[] fileList = {"f1", "f2", "f3", "f4"};
        int writeCount = 0,
            totalRequests = 10;

        Random rand = new Random();

        Instant instant = Instant.now();

        Applog.init();

        if (args.length < 1) {
            throw new InvalidParameterException("Incorrect number of parameters for program");
        }
        
        Client client = new Client(args[0]);

        if (args.length == 2) {
            totalRequests = Integer.parseInt(args[1]);
        }

        // Load server config from file
        client.loadConfig("config.txt");

        for (int i = 0; i < totalRequests; i++) {
            // Randomly select an object
            String key = fileList[rand.nextInt(fileList.length)];

            // Calculate hash
            Integer keyHash = key.hashCode() % client.serverList.size();

            // Decide whether to read/write
            Integer rwbit = rand.nextInt(2);

            List<Integer> serverIndices = new ArrayList<>(
                Arrays.asList(keyHash, (keyHash + 1) % client.serverList.size(), (keyHash + 2) % client.serverList.size())
            );            

            // Send read request
            if (rwbit == 0) {                
                LOGGER.info(String.format("Executing read request"));

                Collections.shuffle(serverIndices);

                for (Integer sidx : serverIndices) {
                    selectedServer = client.serverList.get(sidx);

                    // TODO: Handle failure when connection to server fails. Try next server
                    try {
                        reqSocket = new Socket(selectedServer.ip, selectedServer.port);
                    }
                    catch (ConnectException ex) {
                        LOGGER.info(String.format("Unable to connect to server %s for reading %s", selectedServer.id, key));

                        continue;
                    }

                    // Create a buffer to send messages
                    writer = new PrintWriter(reqSocket.getOutputStream(), true);
        
                    // Create a buffer to receive messages
                    reader = new BufferedReader(new InputStreamReader(reqSocket.getInputStream()));

                    String readRequest = String.format("CLIENT:%s:READ:%s", client.id, key);

                    LOGGER.info(String.format(
                        "Client %s reading object %s from server %s",
                        client.id,
                        key,
                        selectedServer.id
                    ));

                    
                    writer.println(readRequest);

                    String response = reader.readLine();

                    String[] params = response.split(":", 2);

                    if (params[0].equals("ACK")) {
                        LOGGER.info(
                            String.format("Server %s: value of object %s : %s", selectedServer.id, key, params[1])
                        );

                        reqSocket.close();

                        break; // Successful read response from any one server is sufficient
                    }
                    else {
                        LOGGER.info(String.format("%s receives a failure from %s: %s", client.id, selectedServer.id, params));
                    }
        
                    // Clean up socket
                    reqSocket.close();
                }
            }
            else { // Send write request
                LOGGER.info("Executing write request");

                long ts = Instant.now().toEpochMilli();

                String value = String.format("Client %s write count %s", client.id, writeCount++);

                for (Integer sidx : serverIndices) {
                    selectedServer = client.serverList.get(sidx);

                    try {
                        reqSocket = new Socket(selectedServer.ip, selectedServer.port);
                    }
                    catch (ConnectException ex) {
                        LOGGER.info(String.format("Unable to connect to server %s for writing %s:%s", selectedServer.id, key, value));

                        continue;
                    }

                    // Create a buffer to send messages
                    writer = new PrintWriter(reqSocket.getOutputStream(), true);
        
                    // Create a buffer to receive messages
                    reader = new BufferedReader(new InputStreamReader(reqSocket.getInputStream()));

                    String writeRequest = String.format("CLIENT:%s:WRITE:%s:%s:%s", client.id, key, value, ts);

                    LOGGER.info(String.format(
                        "Client %s writing object %s, value %s to server %s at %s",
                        client.id,
                        key,
                        value,
                        selectedServer.id,
                        ts
                    ));
                    
                    writer.println(writeRequest);

                    String response = reader.readLine();

                    String[] params = response.split(":", 2);

                    if (params[0].equals("ACK")) {
                        LOGGER.info(
                            String.format("Server %s: value of object %s : %s", selectedServer.id, key, params[1])
                        );
                    }
                    else {
                        LOGGER.info(String.format("%s receives a failure from %s: %s", client.id, selectedServer.id, params));
                    }
        
                    // Clean up socket
                    reqSocket.close();
                }
            }
        }
    }
}