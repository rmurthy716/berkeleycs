package simpledb;

import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
	other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * field for capacity of bufferpool
     **/ 
    private int capacity;

    /**
     * Maps PageId to page and stores pages in the bufferpool
     **/
    private Map<PageId, Page> BufferPoolPageMap;
    private LinkedList<PageId> LRU;
    private LockManager locks;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.capacity = numPages;
	BufferPoolPageMap = new HashMap<PageId, Page>(capacity);
	LRU = new LinkedList<PageId>();
	this.locks = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public synchronized Page getPage(TransactionId tid,
				     PageId pid,
				     Permissions perm)
        throws TransactionAbortedException, DbException {
	long initialTime = System.currentTimeMillis();
	while (!locks.setLock(tid, pid, perm)) {
	    long currentTime = System.currentTimeMillis();
	    if (currentTime - initialTime >= 200) {
		throw new TransactionAbortedException();
		//assume deadlock and throw exception 
		//since transaction has waited for 2s
	    }
	    try { this.wait(10); }
	    catch (InterruptedException e) {}

	}
	if (BufferPoolPageMap.containsKey(pid)) {
	    updateLRU(pid);
	    return BufferPoolPageMap.get(pid); //page in BufferPool
	}
	else {
	    //page not in BufferPool
	    DbFile file = Database.getCatalog()
		.getDbFile(pid.getTableId());
	    HeapPage pageCandidate = (HeapPage) file.readPage(pid);
	    if (pageCandidate.getId().equals(pid)) {
		updateLRU(pid);
		while (BufferPoolPageMap.size() >= capacity) {
		    evictPage();
		}
		BufferPoolPageMap.put(pid, pageCandidate);
		return pageCandidate;
	    }
	    //page not in disk 
	    throw new DbException("Page not in database.");
	}
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public synchronized void releasePage(TransactionId tid, PageId pid) {
	locks.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return locks.holdsLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
	if (commit == true)
	    for (PageId e : locks.heldPages(tid)) {
		flushPage(e);
		releasePage(tid, e);
	    }
	else {
	    for (PageId e : locks.heldPages(tid)) {
		HeapPage mypage = (HeapPage) BufferPoolPageMap.get(e);
		if (mypage != null) {
		    Page oldPage = mypage.getBeforeImage();
		    DbFile file	= Database.getCatalog()
			.getDbFile(e.getTableId());
		    file.writePage(oldPage);
		    releasePage(tid, e);
		}
	    }
	}
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
	DbFile myfile = Database.getCatalog().getDbFile(tableId);
	ArrayList<Page> ret = myfile.insertTuple(tid, t);
	for (Page p : ret) p.markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
	int tableId = t.getRecordId().getPageId().getTableId();
	DbFile myfile = Database.getCatalog().getDbFile(tableId);
	Page ret = myfile.deleteTuple(tid, t);
	ret.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
	for (PageId p : BufferPoolPageMap.keySet()) flushPage(p);
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
	LRU.remove(pid);
	BufferPoolPageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
	HeapPage mypage = (HeapPage) BufferPoolPageMap.get(pid);
	if (mypage == null) return;
	if (mypage.isDirty() != null) {
	    DbFile file	= Database.getCatalog().getDbFile(pid.getTableId());
	    file.writePage(mypage);
	    mypage.markDirty(false, null);
	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid)
	throws IOException {
	for (PageId e : locks.heldPages(tid)) flushPage(e);
    }

    private synchronized void updateLRU(PageId pid) {
	if (LRU.contains(pid)) {
	    LRU.remove(pid);
	    LRU.add(pid);
	}
	else LRU.add(pid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
	for (PageId e : BufferPoolPageMap.keySet()) {
	    HeapPage mypage = (HeapPage) BufferPoolPageMap.get(e);
	    if (mypage.isDirty() == null) {
		discardPage(e);
		return;
	    }
	}
	throw new DbException("No pages to evict!");
    }
}
