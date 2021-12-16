package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IntegerAggregatorOpIterator extends Operator {

    private List<Tuple> tuples;

    private Iterator<Tuple> rewindTupleIterator;

    private Iterator<Tuple> tupleIterator;

    private Type groupByFieldType;

    private Map<Field, List<Integer>> map;

    private Aggregator.Op operator;

    private TupleDesc tupleDesc;

    public IntegerAggregatorOpIterator(Map<Field, List<Integer>> map, Type groupByFieldType, Aggregator.Op operator){
        this.map = map;
        this.groupByFieldType = groupByFieldType;
        this.operator = operator;
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
                    tuple.setField(1, new IntField(getOperateRe(it)));
                    tuples.add(tuple);
                }
        );
        tupleIterator = tuples.iterator();
        rewindTupleIterator = tuples.iterator();
    }

    private Integer getOperateRe(Field field){
        if (operator == Aggregator.Op.SUM)
            return map.get(field).stream().mapToInt(it -> it).sum();
        else if (operator == Aggregator.Op.MIN)
            return map.get(field).stream().mapToInt(it -> it).min().orElse(-1);
        else if (operator == Aggregator.Op.MAX)
            return map.get(field).stream().mapToInt(it -> it).max().orElse(-1);
        else if (operator == Aggregator.Op.AVG)
            return ((Double)map.get(field).stream().mapToInt(it -> it).average().orElse(-1)).intValue();
        else if (operator == Aggregator.Op.COUNT)
            return ((Long)map.get(field).stream().mapToInt(it -> it).count()).intValue();
        else {
            return -1;
        }
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