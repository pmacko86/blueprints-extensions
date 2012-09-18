package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BdbPropertyDataBinding extends TupleBinding<BdbPropertyData> {

    public void objectToEntry(BdbPropertyData object, TupleOutput to) {
    	to.writeString(object.pkey);
    	try {
    		new ObjectOutputStream(to).writeObject(object.value);
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
    	}
    }

    public BdbPropertyData entryToObject(TupleInput ti) {
    	BdbPropertyData object = new BdbPropertyData();
    	object.pkey = ti.readString();
    	try {
        	object.value = new ObjectInputStream(ti).readObject();
    	} catch (RuntimeException e) {
    		throw e;
    	} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
    	}
    	return object;
    }
} 
