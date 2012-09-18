package com.tinkerpop.blueprints.extensions.impls.bdb;

import java.util.Set;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.extensions.impls.bdb.util.BdbPropertyDataBinding;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public abstract class BdbElement implements Element {
	
    final protected static BdbPropertyDataBinding propertyDataBinding = new BdbPropertyDataBinding();

    public abstract Object getId();
    
    public abstract Object getProperty(final String propertyKey);
    
    public abstract Set<String> getPropertyKeys();
    
    public abstract void setProperty(final String propertyKey, final Object value);
    
    public abstract Object removeProperty(final String propertyKey);

}
