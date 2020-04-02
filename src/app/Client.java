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

    public static Integer countNonNullItems(List<? extends Object> arr) {
        int count = 0;

        for (Object item :  arr) {
            if (item != null) {
                count++;
            }        
        }

        return count;
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
                LOGGER.info(String.format("sending read request"));

                Collections.shuffle(serverIndices);

                for (Integer sidx : serverIndices) {
                    selectedServer = client.serverList.get(sidx);

                    // TODO: Handle failure when connection to server fails. Try next server
                    try {
                        reqSocket = new Socket(selectedServer.ip, selectedServer.port);
                    }
                    catch (ConnectException ex) {
                        LOGGER.info(String.format("unable to connect to server %s for reading %s", selectedServer.id, key));

                        continue;
                    }

                    // Create a buffer to send messages
                    writer = new PrintWriter(reqSocket.getOutputStream(), true);
        
                    // Create a buffer to receive messages
                    reader = new BufferedReader(new InputStreamReader(reqSocket.getInputStream()));

                    String readRequest = String.format("CLIENT:%s:READ:%s", client.id, key);

                    LOGGER.info(String.format(
                        "client %s reading object %s from server %s",
                        client.id,
                        key,
                        selectedServer.id
                    ));

                    
                    writer.println(readRequest);

                    String response = reader.readLine();

                    String[] params = response.split(":", 2);

                    if (params[0].equals("ACK")) {
                        LOGGER.info(
                            String.format("server %s: value of object %s : %s", selectedServer.id, key, params[1])
                        );

                        reqSocket.close();

                        break; // Successful read response from any one server is sufficient
                    }
                    else {
                        LOGGER.info(String.format("received read failure from %s for request %s - %s", selectedServer.id, readRequest, response));
                    }
        
                    // Clean up socket
                    reqSocket.close();
                }
            }
            else { // Send write request
                LOGGER.info("sending write request...");

                long ts = Instant.now().toEpochMilli();

                String value = String.format("client %s write count %s", client.id, writeCount++);

                List<Channel> serverChnls = new ArrayList<>();

                for (Integer sidx : serverIndices) {
                    selectedServer = client.serverList.get(sidx);

                    Channel chnl = null;

                    try {
                        chnl = new Channel(selectedServer.ip, selectedServer.port);
                    }
                    catch (ConnectException ex) {
                        LOGGER.info(String.format("unable to connect to server %s for writing %s:%s", selectedServer.id, key, value));
                    }
                    finally {
                        serverChnls.add(chnl);
                    }
                }

                if (countNonNullItems(serverChnls) < 2) { // ABORT. Not enough replicas available
                    String abortRequest = String.format("CLIENT:%s:ABORT:%s:%s:%s", client.id, key, value, ts);
                
                    for (Integer sidx : serverIndices) {
                        selectedServer = client.serverList.get(sidx);

                        Channel chnl = serverChnls.get(sidx);
    
                        if (chnl == null) continue;
    
                        LOGGER.info(String.format(
                            "client %s aborting write to object %s, value %s to server %s at %s",
                            client.id,
                            key,
                            value,
                            selectedServer.id,
                            ts
                        ));
                        
                        chnl.send(abortRequest);
                    }
    
                    for (Integer sidx : serverIndices) {
                        selectedServer = client.serverList.get(sidx);

                        Channel chnl = serverChnls.get(sidx);
    
                        if (chnl == null) continue;
    
                        String response = chnl.recv();
    
                        String[] params = response.split(":", 2);
    
                        if (params[0].equals("ACK")) {
                            LOGGER.info(
                                String.format("sucessful abort to %s for object %s", selectedServer.id, key)
                            );
                        }
                        else {
                            LOGGER.info(String.format("received abort failure from %s for request %s - %s", selectedServer.id, abortRequest, response));
                        }
            
                        // Clean up socket
                        chnl.close();
                    }
                } else {
                    String writeRequest = String.format("CLIENT:%s:WRITE:%s:%s:%s", client.id, key, value, ts);
                
                    for (Integer sidx : serverIndices) {
                        selectedServer = client.serverList.get(sidx);

                        Channel chnl = serverChnls.get(sidx);
    
                        if (chnl == null) continue;
    
                        LOGGER.info(String.format(
                            "client %s writing object %s, value %s to server %s at %s",
                            client.id,
                            key,
                            value,
                            selectedServer.id,
                            ts
                        ));
                        
                        chnl.send(writeRequest);
                    }
    
                    for (Integer sidx : serverIndices) {
                        selectedServer = client.serverList.get(sidx);
                        
                        Channel chnl = serverChnls.get(sidx);
    
                        if (chnl == null) continue;
    
                        String response = chnl.recv();
    
                        String[] params = response.split(":", 2);
    
                        if (params[0].equals("ACK")) {
                            LOGGER.info(
                                String.format("sucessful write to %s for object %s", selectedServer.id, key)
                            );
                        }
                        else {
                            LOGGER.info(String.format("received write failure from %s for request %s - %s", selectedServer.id, writeRequest, response));
                        }
            
                        // Clean up socket
                        chnl.close();
                    }
                }

            }
        }
    }
}