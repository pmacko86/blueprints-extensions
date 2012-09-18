package com.tinkerpop.blueprints.extensions.impls.bdb.util;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class BdbEdgeDataBinding extends TupleBinding<BdbEdgeData> {

    public void objectToEntry(BdbEdgeData object, TupleOutput to) {
    	to.writeString(object.label);
    	to.writeLong(object.id);
    }

    public BdbEdgeData entryToObject(TupleInput ti) {
    	BdbEdgeData object = new BdbEdgeData(ti.readString(), ti.readLong());
    	return object;
    }
} 
