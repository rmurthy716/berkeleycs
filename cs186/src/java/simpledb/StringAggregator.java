package simpledb;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, IntField> ret;
    private IntField nogroup;
    private int count;
    private TupleDesc td;

    public StringAggregator(int gbfield,
			    Type gbfieldtype,
			    int afield,
			    Op what) {
	if (what != Aggregator.Op.COUNT) 
	    throw new IllegalArgumentException("nope");
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	this.what = what;
	ret = new HashMap<Field, IntField>();
	count = 0;
	nogroup = null;	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
	count++;
	if (gbfield == Aggregator.NO_GROUPING) {
	    nogroup = new IntField(count);
	    return;
	}
	else {
	    Field ind = tup.getField(gbfield);
	    if (!ret.containsKey(ind))
		ret.put(ind, new IntField(1));
	    else {
		IntField oldval = ret.get(ind);
		ret.put(ind, new IntField(oldval.getValue() + 1));
	    }
	    return;
	}
    }

    //Make list of tuples to make the dbiterator simple
    private ArrayList<Tuple> maketuples() {
	ArrayList<Tuple> r = new ArrayList<Tuple>();
	if (gbfield == Aggregator.NO_GROUPING) {
	    TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE },
					 new String[] { "aggregateValue" });
	    this.td = td;
	    Tuple newtup = new Tuple(td);
	    newtup.setField(0, nogroup);
	    r.add(newtup);
	}
	else {
	    TupleDesc td = new TupleDesc(new Type[] { gbfieldtype,
						      Type.INT_TYPE },
					 new String[] { "groupName",
							"aggregateValue" });
	    this.td = td;
	    Iterator<Field> i = ret.keySet().iterator();
	    while (i.hasNext()) {
		Field curkey = i.next();
		IntField curval = ret.get(curkey);
		Tuple newtup = new Tuple(td);
		newtup.setField(0, curkey);
		newtup.setField(1, curval);
		r.add(newtup);
	    }
	}
	return r;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
	ArrayList<Tuple> a = maketuples();
	return new TupleIterator(this.td, a);
    }
}
