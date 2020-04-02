package app;

import java.net.*;
import java.rmi.UnexpectedException;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.security.*;
import java.time.Instant;
import java.util.concurrent.*;

import javax.naming.NameNotFoundException;

/**
 * The primary class for running server instance. 
 */
public class Server extends Node {
    public static long externalTime;
    List<Node> serverList = new ArrayList<Node>();

    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());
    private static ServerSocket serverSocket;

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);
    }

    /**
     * Load and maintain configuration of other servers in cluster as a list. Skips adding own
     * config to list.
     * 
     * @param fileName file to load config from
     */
    public void loadConfig(String fileName) throws FileNotFoundException, IOException {
        BufferedReader inputBuffer = new BufferedReader(new FileReader(fileName));
        String line;
        String[] params;

        LOGGER.info("loading servers from config file");

        while ((line = inputBuffer.readLine()) != null) {
            params = line.split(" ");

            if (!params[0].equals(this.id)) { // Skip adding itself to the server list
                LOGGER.info(String.format("found server %s, ip=%s, port=%s", params[0], params[1], params[2]));

                this.serverList.add(new Node(params[0], params[1], Integer.parseInt(params[2])));
            }
        }

        inputBuffer.close();
    }

    /**
     * Entry point for server
     * 
     * @param args[0] server id to uniquely identify server
     * @param args[1] ip for server to bind and listen on. TODO; remove, ip address not required for binding
     * @param args[2] port for server to bind and listen on
     */
    public static void main(String[] args) throws IOException {
        // Sets the thread pool size. TODO: make this parameter dynamic
        int MAX_POOL_SIZE = 7;

        if (args.length != 3) {
            throw new InvalidParameterException("required parameters <servername> <ip> <port>");
        }

        // Initialize logger
        Applog.init();

        // Create an instance of Server and store connection information
        Server selfServer = new Server(args[0], args[1], Integer.parseInt(args[2]));

        // Capture current system time
        Instant instant = Instant.now();

        LOGGER.info(String.format("server %s starts at time: %s", selfServer.id, instant.toEpochMilli()));

        // Get list of available file servers from config.txt file TODO: remove hard coded values
        selfServer.loadConfig("config.txt");

        // Create a thread pool
        final ExecutorService service = Executors.newFixedThreadPool(MAX_POOL_SIZE);

        // Create a socket and bind to port. Listens on all ip addresses of host
        Server.serverSocket = new ServerSocket(selfServer.port);

        while (true) {
            // Listen for incoming connection requests
            Socket clientSocket = Server.serverSocket.accept();

            // Create a channel 
            Channel clientChannel = new Channel(clientSocket);

            LOGGER.info(String.format("received connection request from ip=%s, port=%s",
                clientSocket.getInetAddress(),
                clientSocket.getPort()
            ));

            requestHandler callobj = new requestHandler(
                clientChannel,
                selfServer
            );

            // Call thread to handle client connection
            service.submit(callobj);
        }
    }
}

class requestHandler implements Callable<Integer> {
    private Channel requesterChannel;
    Server owner;
    String requesterId,
        requesterType;
    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());

    
    public requestHandler(Channel chnl, Server own) {
        this.requesterChannel = chnl;
        this.owner = own;
    }

    private void logInfo(String message) {
        LOGGER.info(String.format("%s: %s: %s", this.requesterId, Thread.currentThread().getId(), message));
    }

    private void logSevere(String message, Exception ex) {
        LOGGER.log(Level.SEVERE, String.format("%s: %s: %s", this.requesterId, Thread.currentThread().getId(), message), ex);
    }

    // Read the last line of file. Taken from https://stackoverflow.com/questions/686231/quickly-read-the-last-line-of-a-text-file
    public String getLastLine(String file) throws FileNotFoundException, IOException {
        RandomAccessFile fileHandler = null;

        try {
            fileHandler = new RandomAccessFile(file, "r");
            
            long fileLength = fileHandler.length() - 1;
            
            StringBuilder sb = new StringBuilder();
    
            for(long filePointer = fileLength; filePointer != -1; filePointer--){
                fileHandler.seek( filePointer );

                int readByte = fileHandler.readByte();
    
                if( readByte == 0xA ) {
                    if( filePointer == fileLength ) {
                        continue;
                    }
                    break;
    
                } else if( readByte == 0xD ) {
                    if( filePointer == fileLength - 1 ) {
                        continue;
                    }
                    break;
                }
    
                sb.append((char) readByte);
            }
    
            String lastLine = sb.reverse().toString();

            return lastLine;
        } 
        finally {
            if (fileHandler != null ) {
                try {
                    fileHandler.close();
                } catch (IOException e) {
                    /* ignore */
                }
            }
        }
    }

    /**
     * Entry point for thread. Handles request, identifies requester and calls respective handler.
     */
    public Integer call() throws IOException, FileNotFoundException {
        String request = this.requesterChannel.recv();

        String[] params = request.split(":");

        this.requesterType = params[0];
        this.requesterId = params[1];
        String action = params[2];
        String obj = params[3];

        if (this.requesterType.equals("CLIENT")) {
            this.logInfo(String.format("request from client with identifier %s", request));

            String value;

            if (action.equals("READ")) {
                try {
                    value = this.clientReadHandler(obj);
  
                    this.logInfo(String.format("server %s sends a successful ack to client %s", this.owner.id, this.requesterId));
    
                    // Send acknowledgement to client for successful task execution
                    this.requesterChannel.send(String.format("ACK:%s", value));
                }
                catch (FileNotFoundException ex) {
                    this.logInfo(String.format("ERR: Object %s not found %s", obj, ex.getMessage()));
    
                    this.requesterChannel.send(String.format("ERR: Object %s not found", obj));
    
                    return 0;
                }
                catch (IOException ex) {
                    this.logInfo(String.format("ERR: Object %s could not be read %s", obj, ex.getMessage()));
    
                    this.requesterChannel.send(String.format("ERR: Object %s could not be read", obj));
    
                    return 0;
                }
                catch (Exception ex) {
                    this.logSevere(ex.getMessage(), ex);
    
                    this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));
    
                    return 0;
                }
            }
            else if (action.equals("WRITE")) {
                try {
                    this.clientWriteHandler(obj, params[4], params[5]);
  
                    this.logInfo(String.format("server %s sends a successful ack to client %s", this.owner.id, this.requesterId));
    
                    // Send acknowledgement to client for successful write to object
                    this.requesterChannel.send("ACK");
                }
                catch (IOException ex) {
                    this.logInfo(String.format("ERR: Object %s unable to write %s", obj, ex.getMessage()));
    
                    this.requesterChannel.send(String.format("ERR: Object %s unable to write", obj));
    
                    return 0;
                }
                catch (Exception ex) {
                    this.logSevere(ex.getMessage(), ex);
    
                    this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));
    
                    return 0;
                }
            }
        }
        else if (this.requesterType.equals("SERVER")) {
            this.logInfo(String.format("request from file server with identifier: %s", request));

            try {
                this.serverHandler();

                this.logInfo(String.format("request %s completed successfully", request));
            }
            catch (Exception ex) {
                this.logSevere(
                    String.format("ERR: %s Failed to handle request from server %s", ex.getMessage(), this.requesterId), 
                    ex
                );

                this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));

                return 0;
            }
        }

        return 1;
    }

    private String clientReadHandler(String obj) throws FileNotFoundException, IOException {
        return getLastLine(obj);
    }

    private void clientWriteHandler(String obj, String value, String ts) throws IOException {
        PrintWriter out = new PrintWriter(new FileWriter(obj, true));

        out.println(value);

        out.close();
    }

    private void serverHandler() {
        
    }
}