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
        int totalRequests = 10;

        Random rand = new Random();

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

            // Send read request
            if (rwbit == 0) {
                List<Integer> serverIndices = new ArrayList<>(
                    Arrays.asList(keyHash, (keyHash + 1) % client.serverList.size(), (keyHash + 1) % client.serverList.size())
                ); 
                
                Collections.shuffle(serverIndices);

                for (Integer sidx : serverIndices) {
                    selectedServer = client.serverList.get(sidx);

                    // TODO: Handle failure when connection to server fails. Try next server
                    reqSocket = new Socket(selectedServer.ip, selectedServer.port);

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
                    }
                    else {
                        LOGGER.info(String.format("%s receives a failure from %s: %s", client.id, selectedServer.id, params));
                    }
        
                    // Clean up socket
                    reqSocket.close();
                }
            }
            else { // Send write request
                LOGGER.info("Reached write request");
            }
        }
    }
}