package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    public ArrayList<TDItem> TDList;
    public int byteSize;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
	return this.TDList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
	TDList = new ArrayList<TDItem>(typeAr.length);
	byteSize = 0;
	for (int i = 0; i < typeAr.length; i++) {
	    TDList.add(new TDItem(typeAr[i],
				  (fieldAr[i] == null) ? "" : fieldAr[i]));
	    byteSize += typeAr[i].getLen();
	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
	this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
	return TDList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
	checkAccess(i);
	return TDList.get(i).fieldName;
    }

    private void checkAccess(int i) throws NoSuchElementException{
	if (i >= numFields() || i < 0)
	    throw new NoSuchElementException();
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
	checkAccess(i);
	return TDList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
	if (name == null) throw new NoSuchElementException();
	for (int i = 0; i < numFields(); i++) {
	    if (getFieldName(i) != null
		&& getFieldName(i).equals(name)) return i;
	}
	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
	return byteSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
	int newlen = td1.numFields() + td2.numFields();
	Type[] rettype = new Type[newlen];
	String[] retname = new String[newlen];
	for (int i = 0; i < td1.numFields(); i++) {
	    rettype[i] = td1.getFieldType(i);
	    retname[i] = td1.getFieldName(i);
	}
	for (int i = 0; i < td2.numFields(); i++) {
	    rettype[i + td1.numFields()] = td2.getFieldType(i);
	    retname[i + td1.numFields()] = td2.getFieldName(i);
	}
        return new TupleDesc(rettype, retname);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null || !(o instanceof TupleDesc)
	    || ((TupleDesc) o).numFields() != this.numFields()) return false;
	for (int i = 0; i < this.numFields(); i++) {
	    if (((TupleDesc) o).getFieldName(i) != this.getFieldName(i)
		|| ((TupleDesc)o).getFieldType(i) != this.getFieldType(i)) return false;
	}
        return true;
    }

    public int hashCode() {
	int ret = 0;
	for (int i = 0; i < numFields(); i++) {
	    ret = 1279 * ret
		+ getFieldName(i).hashCode()
		+ getFieldType(i).getLen();
	}
	return ret;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
	String ret = "[ ";
	for (TDItem s : TDList) {
	    ret += s.toString() + ", ";
	}
	return ret + "]";
    }
}
