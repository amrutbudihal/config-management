package com.mycompany.management;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.util.concurrent.CountDownLatch;

/**
 *
 */
public class FSFileChangeWatcher implements Watcher {

    private static final File file = new File("/Users/abudihal/dynamic-file.txt");
    private long lastModified=0;
    private static final String LINE_SEPARATOR="\n";
    private ZooKeeper zk;
    private static final String ZNODE="/CONFIG/DYNAMIC-FILE";
    private static final int TIMEOUT = 5000;
    private static final CountDownLatch latch = new CountDownLatch(1);
    
    public FSFileChangeWatcher() throws IOException, InterruptedException {
        zk = new ZooKeeper("localhost", TIMEOUT,this);
        latch.await();


        while (true) {
            if (lastModified < file.lastModified()) {
                System.out.println("Last modified timestamp changed.");
                lastModified = file.lastModified();
                //Read the file and notify zookeeper.
                BufferedReader br = null;
                StringBuffer buffer = new StringBuffer(100);
                try {
                    br = new BufferedReader(new FileReader(file));
                    String line ="";
                    while((line = br.readLine())!= null) {
                        buffer.append(line).append(LINE_SEPARATOR);
                    }
                    //Persist changes into ZK node: /CONFIG/DYNAMIC-FILE
                    Stat s = zk.exists(ZNODE,false);
                    if (s != null) {
                        zk.setData(ZNODE, buffer.toString().getBytes(),-1);
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            } else {
                System.out.println("File not changed.");
            }
            Thread.sleep(5000); //check every 5 seconds.
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
           latch.countDown();
        }
    }
    
    
    public static void main(String[] args) throws IOException, InterruptedException {
        new FSFileChangeWatcher();
    }
}
