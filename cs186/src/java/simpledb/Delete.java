package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc result;
    private boolean delete;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
	this.tid = t;
	this.child = child;
	makeResultTupleDesc();
	this.delete = false;
	try{
	    this.child.open();
	}
	catch (Exception e) {
	}
    }

    private void makeResultTupleDesc() {
	Type[] t = new Type[1];
	t[0] = Type.INT_TYPE;
	String[] s = new String[1];
	s[0] = "";
	this.result = new TupleDesc(t, s);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.result;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	super.open();
	this.delete = false;
    }

    public void close() {
        // some code goes here
	super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(delete) return null;
	BufferPool bp = Database.getBufferPool();
	int count = 0;
	while(this.child.hasNext()) {
	    Tuple next = this.child.next();
	    bp.deleteTuple(this.tid, next);
	    count++;
	}
	
	IntField intfield = new IntField(count);
	Tuple resultTuple = new Tuple(this.result);
	resultTuple.setField(0, intfield);
	this.delete = true;
	return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this.child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
	this.child = children[0];
    }

}
