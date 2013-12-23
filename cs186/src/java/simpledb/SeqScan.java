package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;
import java.util.Iterator;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     *identification of transaction performed
     **/
    private TransactionId tid;

    /**
     *identification of table
     **/
    private int tableId;

    /**
     *table alias
     **/
    private String tableAlias;

    /**
     *Iterator of tuples in table with tableId
     **/
    private DbFileIterator i = null;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
	this.tableId = tableid;
	this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableId = tableid;
	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        i = Database.getCatalog().getDbFile(tableId).iterator(tid);
	i.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc tupledesc = Database.getCatalog().getTupleDesc(tableId);
	Iterator<TDItem> iter = tupledesc.iterator();
	//get alias and handle null edge case
	String alias = this.getAlias();
	if(alias == null){
	    alias = this.getTableName();
	}

	//initialize list of fieldNames and fieldTypes
	List<String> fieldNames = new ArrayList<String>();
	List<Type> fieldTypes = new ArrayList<Type>();
	
	while(iter.hasNext()){
	    TDItem item = iter.next();
	    String fName = item.fieldName;
	    Type fType = item.fieldType;
	    //edge case where field name is null
	    if(fName == null){
		fName = "null";
	    }
	    fName = alias + "." + fName;
	    fieldNames.add(fName);
	    fieldTypes.add(fType);
	}
	String[] aliasFNames = fieldNames.toArray(new String[fieldNames.size()]);
	Type[] aliasFTypes = fieldTypes.toArray(new Type[fieldTypes.size()]);
	TupleDesc aliasTupleDesc = new TupleDesc(aliasFTypes, aliasFNames);
	return aliasTupleDesc;
	
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(i == null){
	    return false;
	}
	return i.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (i == null) {
	    throw new NoSuchElementException("Iterator has not been opened.");
	}
	Tuple output = i.next();
	if(output != null){
	    return output;
	}
	else {
	    throw new NoSuchElementException("Tuple not found.");
	}
    }

    public void close() {
        i = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        i.close();
	i.open();
    }
}
