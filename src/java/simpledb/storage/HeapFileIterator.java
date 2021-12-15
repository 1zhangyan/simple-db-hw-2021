package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import sun.lwawt.macosx.CSystemTray;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator{

    private Iterator<Tuple> tupleIterator = null;

    private final TransactionId transactionId;

    private final HeapFile heapFile;

    private Integer pageNumber;

    public HeapFileIterator(TransactionId transactionId, HeapFile heapFile) {
        this.transactionId = transactionId;
        this.heapFile = heapFile;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pageNumber = 0;
        tupleIterator = getPageByNumber(pageNumber).iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (tupleIterator == null) {
            return false;
        }
        if (tupleIterator.hasNext()) {
            return true;
        }
        pageNumber ++;
        try {
            tupleIterator = getPageByNumber(pageNumber).iterator();
        } catch (DbException dbException) {
            System.out.println(dbException.getMessage());
            return false;
        }
        return hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (hasNext()) {
            return tupleIterator.next();
        } else {
            throw new NoSuchElementException();
        }
    }

    private HeapPage getPageByNumber(Integer pageNumber) throws DbException, TransactionAbortedException{
        if (pageNumber < 0 || pageNumber >= heapFile.numPages()) {
            throw new DbException("Page Number is invalid : pageNumber is " + pageNumber + "  numPages is " + heapFile.numPages());
        }
        HeapPageId pageId = new HeapPageId(heapFile.getId(), pageNumber);
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
        return heapPage;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }

    @Override
    public void close() {
        tupleIterator = null;
        pageNumber = 0;
    }

}
