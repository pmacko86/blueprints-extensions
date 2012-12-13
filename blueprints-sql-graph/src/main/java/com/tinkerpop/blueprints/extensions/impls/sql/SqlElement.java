package com.tinkerpop.blueprints.extensions.impls.sql;

import com.tinkerpop.blueprints.Element;

import java.util.Set;

/**
 * @author Daniel Margo (http://www.eecs.harvard.edu/~dmargo)
 */
public abstract class SqlElement implements Element {

    public abstract Object getId();
    
    public abstract long getRawId();
    
    public abstract Object getProperty(final String propertyKey);
    
    public abstract Set<String> getPropertyKeys();
    
    public abstract void setProperty(final String propertyKey, final Object value);
    
    public abstract Object removeProperty(final String propertyKey);

    
    /**
     * Element type
     */
    public enum Type {
    	VERTEX,
    	EDGE
    };
}
