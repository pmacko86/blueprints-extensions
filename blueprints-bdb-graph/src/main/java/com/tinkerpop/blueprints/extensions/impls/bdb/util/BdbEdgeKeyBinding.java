package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BdbEdgeKeyBinding extends TupleBinding<BdbEdgeKey> {

    public void objectToEntry(BdbEdgeKey object, TupleOutput to) {
    	to.writeLong(object.out);
    	to.writeString(object.label);
    	to.writeLong(object.in);
    }

    public BdbEdgeKey entryToObject(TupleInput ti) {
    	BdbEdgeKey object = new BdbEdgeKey(ti.readLong(), ti.readString(), ti.readLong());
    	return object;
    }
} 
