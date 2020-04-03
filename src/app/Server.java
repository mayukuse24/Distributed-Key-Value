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
    Map<String, Node> idToServer = new HashMap<String, Node>();

    public Map<String, Object> objToLock;
    public Map<String, Task> objToLockedTask;

    private final static Logger LOGGER = Logger.getLogger(Applog.class.getName());
    private static ServerSocket serverSocket;

    public static Task NULL_TASK = new Task(null, null, null, null, (long)0);

    Map<String, PriorityBlockingQueue<Task>> objToTaskQueue =
        new ConcurrentHashMap<String, PriorityBlockingQueue<Task>>(Node.fileList.length);

    public Server(String Id, String Ip, int P) {
        super(Id, Ip, P);

        this.objToLock = new ConcurrentHashMap<String, Object>(100);
        this.objToLockedTask = new ConcurrentHashMap<String, Task>(100);

        for (String fileName : Node.fileList) {
            this.objToTaskQueue.put(fileName, new PriorityBlockingQueue<Task>(20, new TaskComparator()));

            this.objToLock.put(fileName, new Object());
        }
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

                this.idToServer.put(params[0], new Node(params[0], params[1], Integer.parseInt(params[2])));
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
        ex.printStackTrace();
    }

    // Read the last line of file. Taken from https://stackoverflow.com/questions/686231/quickly-read-the-last-line-of-a-text-file
    public String getLastLine(String file) throws FileNotFoundException, IOException {
        RandomAccessFile fileHandler = null;

        String filePath = String.format("%s/%s", owner.id, file);

        try {
            fileHandler = new RandomAccessFile(filePath, "r");
            
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
                    this.clientWriteHandler(obj, params[4], Long.parseLong(params[5]), params[6].split(","));
  
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
            else if (action.equals("ABORT")) {
                try {
                    this.logInfo(String.format("server %s sends a successful abort ack to client %s", this.owner.id, this.requesterId));
    
                    // Send acknowledgement to client for successful write to object
                    this.requesterChannel.send("ACK");
                }
                catch (Exception ex) {
                    this.logSevere(ex.getMessage(), ex);
    
                    this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));
    
                    return 0;
                }
            }
        }
        else if (this.requesterType.equals("SERVER")) {
            this.logInfo(String.format("request from file server with identifier %s", request));

            if (action.equals("VOTE")) {
                try {
                    this.serverVoteHandler(obj, params[4], Long.parseLong(params[5]));
    
                    this.logInfo(String.format("processed vote for request %s", request));
                }
                catch (Exception ex) {
                    this.logSevere(
                        String.format("ERR: %s Failed to handle vote request %s", ex.getMessage(), request), 
                        ex
                    );

                    this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));
    
                    return 0;
                }    
            }

            String response = this.requesterChannel.recv();
            
            if (response.equals("RELEASE")) {
                try {
                    this.serverReleaseHandler(obj, params[4], Long.parseLong(params[5]));

                    this.logInfo(String.format("confirmed release for request %s", request));
                }
                catch (Exception ex) {
                    this.logSevere(
                        String.format("ERR: %s Failed to handle release request %s", ex.getMessage(), this.requesterId), 
                        ex
                    );
    
                    this.requesterChannel.send(String.format("ERR: %s", ex.getMessage()));
    
                    return 0;
                } 
            }
        }

        return 1;
    }

    private String clientReadHandler(String obj) throws FileNotFoundException, IOException {
        return getLastLine(obj);
    }

    private void clientWriteHandler(String obj, String value, long ts, String[] replicas) throws IOException, InterruptedException {
        Task task = new Task(this.requesterId, this.owner.id, obj, value, ts);

        // Add task to queue
        this.owner.objToTaskQueue.get(obj).add(task);

        boolean executed = false;

        // Keep trying until task succeeds
        while (!executed) {
            // Wait for task to reach head of queue
            while (!this.owner.objToTaskQueue.get(obj).peek().equals(task)) {
                Thread.sleep(10); // TODO: switch to wait-notify pattern
            }

            // Lock on object
            synchronized(this.owner.objToLock.get(obj)) {
                List<Channel> serverChnls = new ArrayList<>();

                // Store task in locked variable for obj
                this.owner.objToLockedTask.put(obj, task);

                // Send vote to replica servers
                for (String serverId : replicas) {
                    if (serverId.equals(this.owner.id)) continue; // Skip self from replica list

                    Node selectedServer = this.owner.idToServer.get(serverId);

                    try {
                        Channel chnl = new Channel(selectedServer.ip, selectedServer.port, selectedServer.id);

                        chnl.send(String.format("SERVER:%s:VOTE:%s:%s:%s", this.owner.id, obj, this.requesterId, ts));

                        serverChnls.add(chnl);
                    }
                    catch (IOException ex) {
                        this.logInfo(String.format("failed to connect to server %s for voting task %s", selectedServer.id, task));
                    }
                }

                int voteCount = 0;

                boolean reject = false;

                // Wait for response from reachable replicas
                for (Channel chnl : serverChnls) {
                    String response = chnl.recv();

                    String[] params = response.split(":");

                    if (params[0].equals("ERR")) {
                        this.logInfo(String.format("server %s failed to process vote for task %s", chnl.id, task));
                    }
                    else if (params[0].equals("ACK")) {
                        if (params[1].equals("ACCEPT")) {
                            voteCount++;

                            this.logInfo(String.format("received accept from %s for task %s", chnl.id, task));
                        }
                        else if (params[1].equals("REJECT")) {
                            reject = true;

                            // TODO: log extra info received for reject
                            this.logInfo(String.format("received reject from %s for task %s", chnl.id, task)); 
                        }
                    }
                }
                
                // If anyone REJECT
                if (reject) {
                    this.logInfo(String.format("task %s rejected, exiting lock", task));

                    for (Channel chnl : serverChnls) {
                        this.logInfo(String.format("sending reject ack for task %s", task));

                        chnl.send("ACK:REJECT");
                    }

                    // Unlock and retry. Note that retry happens by default until executed = true
                    this.owner.objToLockedTask.remove(obj);
                }
                else if (voteCount >= 1) { // If enough replicas ACCEPT
                    this.logInfo(String.format("task %s accepted, executing...", task));

                    // Perform write
                    task.execute();

                    // Remove task from queue
                    this.owner.objToTaskQueue.get(obj).remove(task);

                    // Send release message to reachable replicas. TODO: Convert to multicast function (DRY)
                    for (Channel chnl : serverChnls) {
                        this.logInfo(String.format("sending release for task %s", task));

                        chnl.send("RELEASE");
                    }

                    // Get Ack from all reachable replicas
                    for (Channel chnl : serverChnls) {
                        this.logInfo(String.format("waiting for release ack for task %s", task));

                        String response = chnl.recv();

                        if (!response.equals("ACK:RELEASE")) {
                            this.logInfo(String.format("failed ack response from server %s", chnl.id));
                        }

                        chnl.close();
                    }

                    // Release lock
                    this.owner.objToLockedTask.remove(obj);

                    // Task completed, exit retry loop
                    executed = true;
                }
            }
        }
    }

    private void serverVoteHandler(String obj, String taskOwner, long ts) throws InterruptedException {
        Task voteTask = new Task(taskOwner, null, null, null, ts);

        // Loop until ACCEPT or REJECT
        while (true) {
            // Get locked task
            Task lockedTask = this.owner.objToLockedTask.get(obj);

            // Check if locked task is null, then sleep and try again
            if (lockedTask == null) {
                Thread.sleep(10);

                continue;
            }

            // Check if locked task same as task being voted, if yes send ACK:ACCEPT
            if (lockedTask.equals(voteTask)) {
                this.logInfo(String.format("accepting vote for task %s", voteTask));

                this.requesterChannel.send("ACK:ACCEPT");

                break;
            }

            // Check if task being voted is behind earliest task in queue, if yes send ACK:REJECT
            Task earliestTask = this.owner.objToTaskQueue.get(obj).peek();

            if (earliestTask == null ||
                earliestTask.timestamp < voteTask.timestamp || 
               (earliestTask.timestamp == voteTask.timestamp && earliestTask.ownerId.compareTo(voteTask.ownerId) < 0)) {
            
                this.logInfo(String.format("rejecting vote for task %s", voteTask));
                
                this.requesterChannel.send("ACK:REJECT"); 

                break;
            }

            // Sleep and try again hoping that locked task is same as task being voted
            Thread.sleep(10);
        }
    }

    private void serverReleaseHandler(String obj, String taskOwner, long ts) {
        Task releaseTask = new Task(taskOwner, null, null, null, ts);

        this.logInfo(String.format("Releasing request for task %s", releaseTask));

        // Loop until released task not present in queue
        while (this.owner.objToTaskQueue.get(obj).contains(releaseTask)) continue;

        this.logInfo(String.format("Sending release ack for task %s", releaseTask));
        
        // Send ACK
        this.requesterChannel.send("ACK:RELEASE");
    }
}