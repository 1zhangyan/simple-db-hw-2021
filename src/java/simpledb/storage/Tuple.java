package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private TupleDesc tupleDesc;

    private List<Field> fields;

    private RecordId recordId;

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        fields = new ArrayList<>(2);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (f == null || i < 0 || i > fields.size()) {
            return;
        }
        this.fields.add(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        try {
            return this.fields.get(i);
        } catch (Throwable t) {
            System.out.println(t.getMessage());
            return null;
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder tupleStr = new StringBuilder("");
        this.fields.forEach(
                it -> {
                    if (it != null) {
                        tupleStr.append(it.toString()).append("\t");
                    } else {
                        tupleStr.append("null").append("\t");
                    }
                }
        );
        tupleStr.deleteCharAt(tupleStr.length()-1);
        return tupleStr.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return this.fields.iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        tupleDesc = td;
    }

    public boolean equals(Object o) {
        if (o == null || !(o instanceof Tuple)) {
            return this == o;
        }
        Tuple target = (Tuple)o;
        return (this.toString().equals(target.toString()));
    }
}
