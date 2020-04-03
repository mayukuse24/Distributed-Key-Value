package app;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Node class underlying client and server. Stores information for a node and provides FIFO support.
 */
public class Node {
    public String id, ip;
    public int port;
    static String[] fileList = {"f1", "f2", "f3", "f4"};

    public Node(String Id) {
        this.id = Id;
    }

    public Node(String Id, String Ip, int p) {
        this.id = Id;
        this.ip = Ip;
        this.port = p;
    }
}