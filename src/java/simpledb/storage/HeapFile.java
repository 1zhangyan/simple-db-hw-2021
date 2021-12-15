package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File file;
    TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public Page readPage(PageId pid) {
        Page page;
        HeapPageId heapPageId = (HeapPageId) pid;
        Integer pageSize = BufferPool.getPageSize();
        Integer fileOffset = heapPageId.pageNumber * pageSize;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            byte[] data = new byte[pageSize];
            randomAccessFile.read(data, fileOffset, pageSize);
            randomAccessFile.close();
            page = new HeapPage(heapPageId, data);
        } catch (Throwable t) {
            System.out.println("Can not Read Page from DbFile.");
            System.out.println(t.getMessage());
            return null;
        }
        return page;
    }

    public void writePage(Page page) throws IOException {

        HeapPage heapPage = (HeapPage) page;
        Integer pageSize = BufferPool.getPageSize();
        Integer fileOffset = heapPage.pid.pageNumber * pageSize;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            byte[] data = new byte[pageSize];
            randomAccessFile.write(data, fileOffset, pageSize);
            randomAccessFile.close();
        } catch (Throwable t) {
            System.out.println("Can not Write Page to DbFile.");
            System.out.println(t.getMessage());
            throw new IOException();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return ((Double)Math.floor(file.length() / BufferPool.getPageSize())).intValue();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid,this);
    }

}

