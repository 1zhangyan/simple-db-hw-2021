package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private List<TDItem> TDItems;


    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public boolean equals(Object o){
            if (o == null){
                return this == o;
            }
            if (!(o instanceof TDItem)) {
                return false;
            }
            TDItem taget = (TDItem)o;
            return this.fieldName.equals(taget.fieldName) && this.fieldType.equals(taget.fieldType);


        }
        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;


    public TupleDesc() {
        TDItems = new ArrayList<>();
    }


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
       TDItems = new ArrayList<TDItem>();
       for(int i = 0; i < typeAr.length; i++) {
           TDItems.add(i, new TDItem(typeAr[i], fieldAr[i]));
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
        TDItems = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++) {
            TDItems.add(i, new TDItem(typeAr[i],""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return TDItems.size();
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
       try {
           return TDItems.get(i).fieldName;
       } catch (Throwable t){
           throw new NoSuchElementException();
       }
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
        try {
            return TDItems.get(i).fieldType;
        } catch (Throwable t){
            throw new NoSuchElementException();
        }
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
        int i = 0;
        for(i = 0 ; i < TDItems.size(); i++){
            String filedName = TDItems.get(i).fieldName;
            if (filedName == null? filedName == name: filedName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return  TDItems.stream().mapToInt(TDItem -> TDItem.fieldType.getLen()).sum();
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
        TupleDesc tupleDesc = new TupleDesc();
        tupleDesc.TDItems.addAll(td1.TDItems);
        tupleDesc.TDItems.addAll(td2.TDItems);
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == null? this!= o : !(o instanceof TupleDesc)){
            return false;
        }
        TupleDesc targetTupleDesc = (TupleDesc) o;
        return TDItems.equals(targetTupleDesc.TDItems);
    }

    public int hashCode() {
        return TDItems.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder tupleDescStr = new StringBuilder("");
        this.TDItems.forEach(it -> tupleDescStr.append(it.fieldType).append("("+ it.fieldName +"),"));
        tupleDescStr.deleteCharAt(tupleDescStr.length()-1);
        return tupleDescStr.toString();
    }
}
