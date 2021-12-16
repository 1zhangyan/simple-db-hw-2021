package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StringAggregatorOpIterator extends Operator{
    private List<Tuple> tuples;

    private Iterator<Tuple> rewindTupleIterator;

    private Iterator<Tuple> tupleIterator;

    private Type groupByFieldType;

    private Map<Field, Integer> map;

    private TupleDesc tupleDesc;

    public StringAggregatorOpIterator(Map<Field, Integer> map, Type groupByFieldType){
        this.map = map;
        this.groupByFieldType = groupByFieldType;
        this.tuples = new ArrayList<>();
        tupleDesc = getTupleDesc();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        map.keySet().forEach(
                it -> {
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0, it);
                    tuple.setField(1, new IntField(map.get(it)));
                    tuples.add(tuple);
                }
        );
        tupleIterator = tuples.iterator();
        rewindTupleIterator = tuples.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        tupleIterator = rewindTupleIterator;
    }

    @Override
    public TupleDesc getTupleDesc() {
        Type[] types = {groupByFieldType, Type.INT_TYPE};
        String[] fieldAr = {"GroupByField", "Value"};
        TupleDesc tupleDesc = new TupleDesc(types, fieldAr);
        return tupleDesc;
    }


    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return tupleIterator.next();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {

    }
}
