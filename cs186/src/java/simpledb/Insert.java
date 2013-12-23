package simpledb;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    private TupleDesc result;
    private boolean insert;
    private Tuple shit;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
	this.tid = t;
	this.child = child;
	this.tableid = tableid;
	this.makeResultTuple();
	try {
	    this.child.open();
	}
	catch (Exception e) {}
    }
    
    private void makeResultTuple() {
	Type[] t = new Type[1];
	t[0] = Type.INT_TYPE;
	String[] s = new String[1];
	s[0] = "";
	this.result = new TupleDesc(t, s);
    }

    public TupleDesc getTupleDesc() {
        return this.result;
    }

    public void open() throws DbException, TransactionAbortedException {
	super.open();
	this.insert = false;
    }

    public void close() {
	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
	this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(this.insert) return null;
	
	BufferPool bp = Database.getBufferPool();
	int count = 0;
	while(this.child.hasNext()) {
	    try{
		Tuple nextTuple = child.next();
		count++;
		bp.insertTuple(this.tid, this.tableid, nextTuple);
	    }
	 
	    catch(Exception e) {
		System.exit(1);
	    }
	}
	Tuple resultTuple = new Tuple(this.result);
	IntField intfield = new IntField(count);
	resultTuple.setField(0, intfield);
	this.insert = true;
	return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	this.child = children[0];
    }
}
