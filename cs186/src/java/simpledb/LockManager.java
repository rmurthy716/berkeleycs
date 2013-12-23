package simpledb;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {

    private ConcurrentHashMap<PageId, ArrayList<TransactionId> > sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    private ConcurrentHashMap<TransactionId,
	ArrayList<PageId> > sharedLockPages;
    private ConcurrentHashMap<TransactionId,
	ArrayList<PageId> > exclusiveLockPages;
    
    public LockManager() {
	this.sharedLocks = new ConcurrentHashMap<PageId,
	    ArrayList<TransactionId> >();
	this.exclusiveLocks = new ConcurrentHashMap<PageId,
	    TransactionId>();
	this.sharedLockPages = new ConcurrentHashMap<TransactionId,
	    ArrayList<PageId> >();
	this.exclusiveLockPages = new ConcurrentHashMap<TransactionId,
	    ArrayList<PageId> >();
    }
    
    public synchronized boolean setLock(TransactionId tid,
					PageId pid,
					Permissions perm) {
	if (perm.equals(Permissions.READ_ONLY)) {
	    if (holdsLock(tid, pid)) return true;
	    return setSLock(tid, pid);
	}
	else {
	    return setXLock(tid, pid);
	}
    }
    
    private synchronized boolean setSLock(TransactionId tid, PageId pid) {
	//get transaction id of any exclusive locks on this page id
	TransactionId exTid = this.exclusiveLocks.get(pid);
	//get transaction ids of any shared locks on this page id
	ArrayList<TransactionId> sharedTids = this.sharedLocks.get(pid);
	if(exTid == null || exTid.equals(tid)) {
	    //there are no exclusive locks or if there are the requesting transaction id
	    //already has an exclusive lock thus we allow a shared lock
	    if(sharedTids == null) {
		//this page has no associated transaction ids
		sharedTids = new ArrayList<TransactionId>();
	    }
	    sharedTids.add(tid); //add current tid and update sharedTids
	    this.sharedLocks.put(pid, sharedTids); //update sharedLocks HashMap
	    
	    ArrayList<PageId> sharedPids = this.sharedLockPages.get(tid); //get list of pages associated with transaction 
	    if(sharedPids == null) {
		sharedPids = new ArrayList<PageId>(); //no associated pages so far so create new list
	    }
	    sharedPids.add(pid);//add current page to list
	    this.sharedLockPages.put(tid, sharedPids);//put the list of pages back into field of sharedLockPages
	    return true;//succesfully set lock so return true
	}
	else {
	    //there exists an exclusive lock on this page thus deny shared lock
	    return false;
	}
    }
    
    private synchronized boolean setXLock(TransactionId tid, PageId pid) {
	//some code goes here
	TransactionId exTid = this.exclusiveLocks.get(pid);
	ArrayList<TransactionId> sharedTids = this.sharedLocks.get(pid);
	if(exTid != null && !(exTid.equals(tid))) {
	    return false; //there already exists an exclusive lock that is not this current transaction
	}
	if(sharedTids != null && sharedTids.size() == 1 && !(sharedTids.contains(tid))) {
	    return false; //some other transaction already has a shared lock so we can't grant exclusive lock
	}
	if(sharedTids != null && sharedTids.size() > 1) {
	    return false; //more than one transaction has a shared lock on the page so we can't grant an exclusive lock
	}
	else {
	    //no other tid has an exclusive lock or shared lock
	    //current tid may have a shared lock
	    this.exclusiveLocks.put(pid, tid);
	    ArrayList<PageId> xLockPages = this.exclusiveLockPages.get(tid);
	    if(xLockPages == null) {
		xLockPages = new ArrayList<PageId>();
	    }
	    xLockPages.add(pid);
	    this.exclusiveLockPages.put(tid, xLockPages);
	    return true;
	}
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
	ArrayList<PageId> sharedPages = sharedLockPages.get(tid); //get shared page ids for given transaction
	if(sharedPages != null) {
	    //if there are any pids remove the current one and put the modified list of pids back into sharedLockPages
	    sharedPages.remove(pid);
	    sharedLockPages.put(tid, sharedPages);
	}
	ArrayList<TransactionId> sharedTids = sharedLocks.get(pid); //get shared tids
	if(sharedTids != null) {
	    //it there are any tids remove the current one and put the modified list of tids back into sharedLocks
	    sharedTids.remove(tid);
	    sharedLocks.put(pid, sharedTids);
	}
	ArrayList<PageId> xPages = exclusiveLockPages.get(tid);//get exclusive page ids 
	if(xPages != null) {
	    //if there are any pids remove the current one and put the modified list of pids back into exclusiveLockPages
	    xPages.remove(pid);
	    exclusiveLockPages.put(tid, xPages);
	}
	exclusiveLocks.remove(pid); //only one tid for xLocks so remove it from list of exclusive locks
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
	//check for shared locks
	ArrayList<TransactionId> sharedTids = sharedLocks.get(pid);
	if(sharedTids != null && sharedTids.contains(tid)) {
	    return true;
	}
	//check for exclusive locks
	TransactionId xTid = exclusiveLocks.get(pid);
	if(xTid != null && xTid.equals(tid)) {
	    return true;
	}
	return false;
    }

    public synchronized ArrayList<PageId> heldPages(TransactionId tid) {
	ArrayList<PageId> sharedPages = sharedLockPages.get(tid);
	ArrayList<PageId> xPages = exclusiveLockPages.get(tid);
	ArrayList<PageId> allPages = new ArrayList<PageId>();
	if (sharedPages != null) allPages.addAll(sharedPages);
	if (xPages != null) allPages.addAll(xPages);
	return allPages;
    }
}
