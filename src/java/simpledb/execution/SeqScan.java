package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private DbFileIterator dbFileIterator;
    private boolean isOpen;


    TupleDesc tupleDesc1;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid= tid;
        this.tableid= tableid;
        this.tableAlias= tableAlias;
        this.dbFileIterator = null;
        this.isOpen = false;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.isOpen = false;

        tupleDesc1 = Database.getCatalog().getTupleDesc(tableid);
        String [] schemaFieldNames = new String[tupleDesc1.numFields()];
        Type [] schemaTypes = new Type[tupleDesc1.numFields()];
        for (int i = 0; i < tupleDesc1.numFields(); i++) {
            schemaFieldNames[i] = this.tableAlias + "." + tupleDesc1.getFieldName(i);
            schemaTypes[i] = tupleDesc1.getFieldType(i);
        }
        tupleDesc1=  new TupleDesc(schemaTypes, schemaFieldNames);
    }


    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

//    Get the database file associated with the tableid using Database.getCatalog().getDatabaseFile(tableid). This retrieves the DbFile object responsible for storing the table's data.
//    Retrieve the iterator associated with the DbFile using iterator(tid). This DbFileIterator is responsible for iterating through the tuples in the table.
//    Open the iterator using dbFileIterator.open(). This operation prepares the iterator to begin iterating through the tuples in the table.
    public void open() throws DbException, TransactionAbortedException {
        if (isOpen) {
            throw new DbException("");
        }
        this.dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        this.dbFileIterator.open();
        this.isOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {

        return tupleDesc1;

    }
    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!isOpen) {
            throw new IllegalStateException("");
        }
        return this.dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (!isOpen) {
            throw new IllegalStateException("");
        }
        return this.dbFileIterator.next();
    }

    public void close() {
        // some code goes here
        isOpen = false;
        this.dbFileIterator.close();
    }

<<<<<<< HEAD
    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();

=======
    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        close();
        open();
>>>>>>> b3b52df001009f014bfe25dc430e7fec18c4db5d
    }

}
