package knn.graph.impl;

import java.io.Serializable;

import knn.graph.INode;

/**
 *
 * @author Thibault Debatty
 * @param <T> Type of value field
 */
public class Node<T> implements Serializable, INode<String, T> {

	private static final long serialVersionUID = -6166363793961464459L;
	public String id = "";
    public T value;
    public String stringValue;

    public Node() {
    }

    public Node(String id) {
        this.id = id;
    }

    public Node(String id, T value) {
        this.id = id;
        this.value = value;
    }

    public Node(String id, T value, String stringValue_) {
        this.id = id;
        this.value = value;
        this.stringValue = stringValue_;
    }

    @Override
    public String toString() {
        return "(" + id + " => " + value.toString() + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (! other.getClass().isInstance(this)) {
            return false;
        }

        return this.id.equals(((Node<T>) other).id);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = (83 * hash) + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

	@Override
	public String getId() {
		return id;
	}

	@Override
	public T getValue() {
		return value;
	}

}
