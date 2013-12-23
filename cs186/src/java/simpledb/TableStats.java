package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    /**
     * List of private fields for class
     */
    
    //Map for maximum values of fields
    //Takes field name to maximum value found for each field
    private HashMap<Integer, Integer> maxMap;
    //HashMap for minimum values
    //Takes name to minimum value found for each field
    private HashMap<Integer, Integer> minMap;
    
    //HashMap of histograms for integer fields
    //Takes field name to int histogram
    private HashMap<Integer, IntHistogram> setIntHistograms;
    //HashMap of histograms for string fields
    //Takes field name to string histogram
    private HashMap<Integer, StringHistogram> setStringHistograms;
 
    private int tableId;
    private int ioCostPerPage;
    private int countTuples;
    
    //HeapFile for table
    private HeapFile hfile;
    

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here

	
	this.minMap = new HashMap<Integer, Integer>();
	this.maxMap = new HashMap<Integer, Integer>();
	this.setIntHistograms = new HashMap<Integer, IntHistogram>();
	this.setStringHistograms = new HashMap<Integer, StringHistogram>();
	this.tableId = tableid;
	this.ioCostPerPage = ioCostPerPage;
	this.hfile = (HeapFile)Database.getCatalog().getDbFile(tableid);
	//private helper function that computes minimum and maximums of each field in table
	//also gets total count of tuples in the table
	this.getStatistics();
	//private helper function that makes histogram for each field in table 
	//and adds value of each field in each tuple to the appropriate histogram 
	this.makeHistograms();
    }
    
    
    private void makeHistograms() {
	//get TupleDesc from Heap File
	TupleDesc tupleDesc = this.hfile.getTupleDesc();
	//create new iterator for heapfile
	DbFileIterator iterator = this.hfile.iterator(new Transaction().getId());
	try {
	    iterator.open();
	    while(iterator.hasNext()){
		Tuple tuple = iterator.next();
		for(int i = 0; i<tupleDesc.numFields(); i++) {
		    String fieldName = tupleDesc.getFieldName(i);
		    if(tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
			//field is an integer so retrieve it
			IntField integerField = (IntField) tuple.getField(i);
			int value = integerField.getValue();
			
			//check if our set of histograms contains this field and if none exists create one
			if(!this.setIntHistograms.containsKey(i)) {
			    this.setIntHistograms.put(i, new IntHistogram(NUM_HIST_BINS, minMap.get(i), maxMap.get(i)));
			}
			//get current histogram add value and put back into our set of histograms
			IntHistogram currentIntHistogram = this.setIntHistograms.get(i);
			currentIntHistogram.addValue(value);
			setIntHistograms.put(i, currentIntHistogram);
		    }
		    else {
			//field value must be a string
			StringField stringField = (StringField) tuple.getField(i);
			String value = stringField.getValue();
			
			//check if our set of histograms contains this field and if none exists create one
			if(!this.setStringHistograms.containsKey(i)) {
			    this.setStringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
			}
			
			StringHistogram currentStringHistogram = this.setStringHistograms.get(i);
			currentStringHistogram.addValue(value);
			setStringHistograms.put(i, currentStringHistogram);
		    }
		}
	    }
	}
	catch (DbException e) {
	}
	catch (TransactionAbortedException e) {
	}
	iterator.close();
    }

    private void getStatistics() {
	TupleDesc tupleDesc = this.hfile.getTupleDesc();
	DbFileIterator iterator = this.hfile.iterator(new Transaction().getId());
	try {
	    iterator.open();
	    while(iterator.hasNext()) {
		Tuple tuple = iterator.next();
		this.countTuples++;
		for(int i = 0; i < tupleDesc.numFields(); i++) {
		    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
			IntField intField = (IntField)tuple.getField(i);
			String fieldName = tupleDesc.getFieldName(i);

			if (!minMap.containsKey(i)
			    || minMap.get(i) > intField.getValue()) {
			    minMap.put(i, intField.getValue());
			}
			if (!maxMap.containsKey(i)
			    || maxMap.get(i) < intField.getValue()) {
			    maxMap.put(i, intField.getValue());
			}
		    }
		}
	    }
	}
	catch(DbException e) {
	}
	catch(TransactionAbortedException e) {
	}
	iterator.close();
    }



    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        int countPages = this.hfile.numPages();
	return countPages * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.countTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        TupleDesc tupleDesc = this.hfile.getTupleDesc();
	String fieldName = tupleDesc.getFieldName(field);
	if(tupleDesc.getFieldType(field).equals(Type.INT_TYPE)) {
	    IntField intField = (IntField)constant;
	    IntHistogram intHistogram = this.setIntHistograms.get(field);
	    return intHistogram.estimateSelectivity(op, intField.getValue());
	}
	else {
	    StringField stringField = (StringField)constant;
	    StringHistogram stringHistogram = this.setStringHistograms.get(field);
	    return stringHistogram.estimateSelectivity(op, stringField.getValue());
	}
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.countTuples;
    }

}
