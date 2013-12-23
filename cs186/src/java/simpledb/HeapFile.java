package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     * the file that stores the on-disk backing store for this heap
     * file.
     */

    private File f;
    private TupleDesc td;
    private int id;

    private class PageEntry {
	public Page page;
	public TupleDesc td;
	public int freespace;

	public PageEntry(Page page, TupleDesc td) {
	    this.page = page;
	    this.td = td;
	    this.freespace = ((HeapPage) page).getNumEmptySlots() 
		* td.getSize();
	}
    }
    
    private ArrayList<PageEntry> pages;
    
    public HeapFile(File f, TupleDesc td) {
	this.f = f;
	this.td = td;
	this.id = f.getAbsoluteFile().hashCode();
	this.pages = new ArrayList<PageEntry>();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
	return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
	return id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
	try {
	    RandomAccessFile s = new RandomAccessFile (getFile(), "rws");
	    int size = BufferPool.PAGE_SIZE;
	    byte[] ret = new byte [size];
	    int offset = pid.pageNumber() * size;
	    s.seek(offset);
	    s.read(ret, 0, size);
	    s.close();
	    return new HeapPage((HeapPageId) pid, ret);
	}
	catch (IOException ex) {
	    return null;
	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
	RandomAccessFile s = new RandomAccessFile (getFile(), "rws");
	try {
	    int size = BufferPool.PAGE_SIZE;
	    int offset = page.getId().pageNumber() * size;
	    s.seek(offset);
	    s.write(page.getPageData(), 0, size);
	    page.markDirty(false, null);
	}
	catch (IOException ex) {}
	finally {
	    s.close();
	}
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
	return (int) Math.floor(f.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
	boolean added = false;
	ArrayList<Page> ret = new ArrayList<Page>();
	for (int i = 0; i < numPages(); i++) {
	    HeapPageId pageId = new HeapPageId(getId(), i);
	    HeapPage mypage = (HeapPage) Database
		.getBufferPool().getPage(tid,
					 pageId,
					 Permissions.READ_WRITE);
	    if (mypage.getNumEmptySlots() > 0) {
		mypage.insertTuple(t);
		ret.add(mypage);
		added = true;
		break;
	    }
	}
	if (!added) {
	    HeapPageId pageId = new HeapPageId(getId(), numPages());
	    HeapPage newpage = new HeapPage(pageId,
					    HeapPage.createEmptyPageData());
	    writePage(newpage);
	    HeapPage mypage = (HeapPage) Database
		.getBufferPool().getPage(tid,
					 pageId,
					 Permissions.READ_WRITE);
	    mypage.insertTuple(t);
	    pages.add(new PageEntry(mypage, td));
	    ret.add(mypage);
	}
	return ret;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
	HeapPage mypage = (HeapPage) Database
	    .getBufferPool().getPage(tid,
				     t.getRecordId().getPageId(),
				     Permissions.READ_WRITE);
	mypage.deleteTuple(t);
	return mypage;
    }

    // see DbFile.java for javadocs

   
    public DbFileIterator iterator(TransactionId tid) {
	return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

	private Iterator<Tuple> i = null;
	private TransactionId tid; 
	private int pageNumber;
	
	public HeapFileIterator(TransactionId tid) {
	    this.tid = tid;
	}

	private HeapPage load(int pgNo) throws DbException {
	    PageId pageId = new HeapPageId(getId(), pgNo);
	    try {
		Page page = Database
		    .getBufferPool().getPage(tid, 
					     pageId, 
					     Permissions.READ_ONLY);
		return (HeapPage) page;
	    }
	    catch(TransactionAbortedException e){
		return null;
	    }
	}

	@Override
	public void open() throws DbException, TransactionAbortedException{
	    pageNumber = 0;
	    HeapPage ret = load(pageNumber);
	    if (ret == null) {
		i = new Iterator<Tuple>() {
		    @Override
		    public boolean hasNext() {return false;}
		    @Override
		    public Tuple next() {return null;}
		    @Override
		    public void remove() {
			throw new UnsupportedOperationException();
		    }
		};
		return;
	    }
	    i = ret.iterator();
	}
	
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException{
	    if (i == null) {
		return false;
	    }
	    if (i.hasNext()) {
		return true;
	    }
	    if (pageNumber < numPages() - 1) {
		pageNumber++;
		HeapPage nextpage = load(pageNumber);
		if (nextpage == null) return false;
		i = nextpage.iterator();
		return i.hasNext();
	    }
	    return false;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
	    if (i == null) { 
		throw new NoSuchElementException("Need to call open!");
	    }
	    if (i.hasNext()) {
		Tuple ret = i.next();
		return ret;
	    }
	    else if(pageNumber < numPages() - 1) {
		
		pageNumber++;
		HeapPage nextpage = load(pageNumber);
		i = nextpage.iterator();
		if (i.hasNext()) return i.next();
	    }
	    throw new NoSuchElementException("No more Tuples.");
	}
	
	@Override
	public void rewind() throws DbException, TransactionAbortedException {
	    close();
	    open();
	}

	@Override
	public void close() {
	    i = null;
	}
    }
}
