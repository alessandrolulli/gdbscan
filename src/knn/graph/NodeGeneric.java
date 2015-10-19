package knn.graph;

import java.io.Serializable;

public class NodeGeneric<TID, T> implements Serializable, INode<TID, T>
{
	private static final long serialVersionUID = 1L;
	private final TID id;
    private final T value;

    public NodeGeneric(TID id, T value) {
        this.id = id;
        this.value = value;
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

        return this.id.equals(((NodeGeneric<TID, T>) other).id);
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = (83 * hash) + (this.id != null ? this.id.hashCode() : 0);
        return hash;
    }

	@Override
	public TID getId() {
		return id;
	}

	@Override
	public T getValue() {
		return value;
	}

}
