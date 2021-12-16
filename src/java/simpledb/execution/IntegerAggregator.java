package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;


    private int groupByField;

    private Type groupByFieldType;

    private int aggregateFiled;

    private Op operator;

    private Map<Field, List<Integer>> aggregateMap;



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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFiled = afield;
        this.operator = what;
        aggregateMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        mergeIntoMap(tup.getField(groupByField), ((IntField)tup.getField(aggregateFiled)).getValue());
    }

    private void mergeIntoMap(Field field, Integer value) {
        List list;
        if (aggregateMap.containsKey(field)) {
            list = aggregateMap.get(field);
        } else {
            list = new ArrayList<Integer>();
        }
        list.add(value);
        aggregateMap.put(field, list);
        return;
    }


    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        IntegerAggregatorOpIterator integerAggregatorOpIterator = new IntegerAggregatorOpIterator(aggregateMap, groupByFieldType, operator);
        return integerAggregatorOpIterator;
    }


}
