package app;

import java.io.*;
import java.util.*;

/**
 * Maintains information regarding a task
 */
public class Task {
    public long timestamp;
    String ownerId, // Client that originally created and sent task
        executorId,
        fileName,
        message;

    public Task(String oid, String eid, String fname, String message, long ts) {
        this.ownerId = oid;
        this.executorId = eid;
        this.fileName = fname;
        this.message = message;
        this.timestamp = ts;
    }

    public void execute() throws IOException {
        // TODO: handle failure when file does not exist
        PrintWriter fileObj = new PrintWriter(
            new FileWriter(String.format("files/%s/%s", this.executorId, this.fileName), true) // TODO: obtain file path via ENV
        );

        fileObj.println(this.message);

        fileObj.close();
    }

    @Override
    public String toString() {
        return String.format("(%s:%s:%s:%s)", ownerId, timestamp, fileName, message);
    }

    @Override
    public boolean equals(Object o) {
        // If the object is compared with itself then return true   
        if (o == this) { 
            return true; 
        } 
  
        // Check if o is an instance of Task or not
        if (!(o instanceof Task)) { 
            return false; 
        } 
        
        Task t = (Task) o; 
          
        // Task is same if timestamp and owner are same  
        return t.timestamp == this.timestamp && t.ownerId.equals(this.ownerId);
    }
};

/**
 * Comparator function to be used for ordering task in the priority queue.
 */
class TaskComparator implements Comparator<Task> {
    // Overriding compare()method of Comparator for tasks by ascending timestamp, ties broken by ascending serverIds
    public int compare(Task t1, Task t2) { 
        if (t1.timestamp > t2.timestamp) {
            return 1;
        }
        else if (t1.timestamp < t2.timestamp) {
            return -1; 
        }
        else {
            if (t1.ownerId.compareTo(t2.ownerId) > 0) {
                return 1;
            }
            else {
                return -1;
            }
        }
    }
} 