package com.mycompany.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 *  checks for any file changes and accordingly gets updates.
 */
public class ZkFileChangeWatcher implements Watcher {
    
    private ZooKeeper zk;
    private static final int TIMEOUT=5000;
    private static final String NODE="/CONFIG";
    private static final String FILE_NODE="/DYNAMIC-FILE";
    private CountDownLatch latch = new CountDownLatch(1);
    
    public ZkFileChangeWatcher() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper("localhost", TIMEOUT, this);
        latch.await();
        
        Stat s = zk.exists(NODE, this);
        if(s == null) {
            zk.create(NODE,null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        s = zk.exists(NODE+FILE_NODE, this);
        if (s == null) {
            //create node if it doesn't exist.
            String createdPath = zk.create(NODE+FILE_NODE, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        System.out.println("Waiting for changes.");
    }
    
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            latch.countDown();
        }

        if(event.getType() == Event.EventType.NodeDataChanged) {
            try {
                byte[] data = zk.getData(NODE+FILE_NODE, this,null);
                System.out.println("Recieved update on data:"+new String(data));
            } catch (KeeperException e) {
                e.printStackTrace();  
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        new ZkFileChangeWatcher();
        Thread.sleep(100000);
    }
}
