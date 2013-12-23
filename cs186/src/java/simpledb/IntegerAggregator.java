package simpledb;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, IntField> ret;
    private HashMap<Field, Integer> countmap;
    private IntField nogroup;
    private int count;
    private TupleDesc td;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
	this.gbfield = gbfield;
	this.gbfieldtype = gbfieldtype;
	this.afield = afield;
	this.what = what;
	ret = new HashMap<Field, IntField>();
	countmap = new HashMap<Field, Integer>();
	count = 0;
	nogroup = null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
	count++;
	if (gbfield == Aggregator.NO_GROUPING) {
	    IntField newval = (IntField) tup.getField(afield);
	    if (what == Aggregator.Op.COUNT) {
		nogroup = new IntField(count);
		return;
	    }
	    if (nogroup == null) {
		nogroup = newval;
		return;
	    }
	    if (what == Aggregator.Op.MIN)
		nogroup = (nogroup.getValue() > newval.getValue())
		    ? newval : nogroup;
	    if (what == Aggregator.Op.MAX)
		nogroup = (nogroup.getValue() < newval.getValue())
		    ? newval : nogroup;
	    if (what == Aggregator.Op.SUM || what == Aggregator.Op.AVG)
		nogroup = new IntField(nogroup.getValue() +
				       newval.getValue());
	    /*if (what == Aggregator.Op.AVG)
		nogroup = new IntField(nogroup.getValue() +
				       ((newval.getValue()
				       - nogroup.getValue()) / count));*/
	}
	else {
	    IntField newval = (IntField) tup.getField(afield);
	    Field ind = tup.getField(gbfield);
	    if (what == Aggregator.Op.COUNT) {
		if (!ret.containsKey(ind))
		    ret.put(ind, new IntField(1));
		else {
		    IntField oldval = ret.get(ind);
		    ret.put(ind, new IntField(oldval.getValue() + 1));
		}
		return;
	    }
	    if (!ret.containsKey(ind)) {
		ret.put(ind, newval);
		countmap.put(ind, 1);
		return;
	    }
	    if (what == Aggregator.Op.MIN) {
		IntField oldval = ret.get(ind);
		ret.put(ind, 
			(oldval.getValue() < newval.getValue()) 
			? oldval : newval);
	    }
	    if (what == Aggregator.Op.MAX) {
		IntField oldval = ret.get(ind);
		ret.put(ind,
			(oldval.getValue() > newval.getValue())
			? oldval : newval);
	    }
	    if (what == Aggregator.Op.SUM || what == Aggregator.Op.AVG) {
		IntField oldval = ret.get(ind);
		int oldcount = countmap.get(ind);
		countmap.put(ind, oldcount + 1);
		ret.put(ind, new IntField(oldval.getValue() + 
					  newval.getValue()));
	    }
	    /*if (what == Aggregator.Op.AVG) {
		IntField oldval = ret.get(ind);


		ret.put(ind,
			new IntField(oldval.getValue() +
				     ((newval.getValue() -
				       oldval.getValue()) /
				      (oldcount + 1))));
				      }*/
	}
    }

    //Make list of tuples to make the dbiterator simple
    private ArrayList<Tuple> maketuples() {
	ArrayList<Tuple> r = new ArrayList<Tuple>();
	if (gbfield == Aggregator.NO_GROUPING) {
	    TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE },
					 new String[] { "" });
	    this.td = td;
	    Tuple newtup = new Tuple(td);
	    if (what == Aggregator.Op.AVG)
		newtup.setField(0, new IntField(nogroup.getValue() / count));
	    else
		newtup.setField(0, nogroup);
	    r.add(newtup);
	}
	else {
	    TupleDesc td = new TupleDesc(new Type[] { gbfieldtype, 
						      Type.INT_TYPE },
					 new String[] { "", 
							"" });
	    this.td = td;
	    Iterator<Field> i = ret.keySet().iterator();
	    while (i.hasNext()) {
		Field curkey = i.next();
		IntField curval = ret.get(curkey);
		Tuple newtup = new Tuple(td);
		newtup.setField(0, curkey);
		if (what == Aggregator.Op.AVG)
		    newtup.setField(1, new IntField(curval.getValue() /
						    countmap.get(curkey)));
		else
		    newtup.setField(1, curval);
		r.add(newtup);
	    }
	}
	return r;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
	ArrayList<Tuple> a = maketuples();
	return new TupleIterator(this.td, a);
    }
}
