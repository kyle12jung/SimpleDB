package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private int numFields;
    private Type [] types;
    private String [] fieldNames;
    private TDItem [] items;


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

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> fields() {
        return new Iterator<TDItem>() {
            private int currentIndex = 0;
            @Override
            public boolean hasNext() {
                return currentIndex < numFields;
            }
            @Override
            public TDItem next() {
                return items[currentIndex++];
            }
        };
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
        // some code goes here
        this.numFields = fieldAr.length;
        this.types = new Type[numFields];
        this.fieldNames = new String[numFields];
        this.items = new TDItem[numFields];

        for (int i = 0; i < numFields; i++) {
            this.types[i] = typeAr[i];
            this.fieldNames[i] = fieldAr[i];
            this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
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
        // some code goes here
        this.numFields = typeAr.length;
        this.types = new Type[numFields];
        this.fieldNames = new String[numFields];
        this.items = new TDItem[numFields];

        for (int i = 0; i < numFields; i++) {
            this.types[i] = typeAr[i];
            this.fieldNames[i] = null;
            this.items[i] = new TDItem(typeAr[i], fieldNames[i]);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.numFields;
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
        // some code goes here
        if (i < this.numFields && i >= 0) {
            return fieldNames[i];
        } else {
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
        // some code goes here
        if (i < this.numFields() && i >= 0) {
            return types[i];
        } else {
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
        for (int i = 0; i < fieldNames.length; i++) {
            if ((name != null && name.equals(fieldNames[i])) || (name == null && fieldNames[i] == null)) {
                return i;
            }
        }
        throw new NoSuchElementException("Field name not found: " + name);
    }



    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int totalSize = 0;
        for (Type type: types) {
            totalSize += type.getLen();
        }
        return totalSize;
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
        // some code goes here
        Type[] mergedTypes = new Type[td1.numFields()+td2.numFields()];
        String[] mergedFieldNames = new String[td1.numFields()+td2.numFields()];
        // first table
        for (int i = 0; i < td1.numFields(); i++) {
            mergedTypes[i] = td1.getFieldType(i);
            mergedFieldNames[i] = td1.getFieldName(i) != null ? td1.getFieldName(i) : null;
        }
        // second table
        for (int i = td1.numFields(); i < (td1.numFields() + td2.numFields()); i++) {
            mergedTypes[i] = td2.getFieldType(i - td1.numFields());
            mergedFieldNames[i] = td2.getFieldName(i - td1.numFields()) != null ? td2.getFieldName(i - td1.numFields()) : null;
        }

        return new TupleDesc(mergedTypes, mergedFieldNames);
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
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == null) return false;
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc td = (TupleDesc) o;
        if (td.numFields() != this.numFields()) return false;
        for (int i = 0; i < this.numFields(); i++) {
            if (td.getFieldType(i) != this.getFieldType(i)) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        String[] tupleDescString = new String[this.numFields()];
        for (int i = 0; i < tupleDescString.length; i++) {
            tupleDescString[i] = getFieldType(i) + "(" + getFieldName(i) + ")";
        }
        return String.join("", tupleDescString);
    }
}
