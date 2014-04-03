package com.btoddb.fastpersitentqueue.speedtest;

import com.btoddb.fastpersitentqueue.JournalFileMgr;


/**
 * 
 */
public class SpeedWorker implements Runnable {

    private final JournalFileMgr mgr;
    private final long durationOfTest;
    private final int entrySize;

    public SpeedWorker(JournalFileMgr mgr, long durationOfTest, int entrySize) {
        this.mgr = mgr;
        this.entrySize = entrySize;
        this.durationOfTest = durationOfTest;
    }

    @Override
    public void run() {
        
    }
}
