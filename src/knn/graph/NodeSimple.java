package knn.graph;

import java.io.Serializable;

public class NodeSimple<TID, T> implements Serializable, INode<TID, T>
{
	private static final long serialVersionUID = -9041123557036726438L;

	private final TID _id;

	public NodeSimple(TID id_)
	{
		_id = id_;
	}

	@Override
	public TID getId()
	{
		return _id;
	}

	@Override
	public T getValue()
	{
		return null;
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

        return this.getId().equals(((NodeSimple<TID, T>) other).getId());
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
//    	int hash = 5;
//        hash = (83 * hash) + (this.getId() != null ? this.getId().hashCode() : 0);
//        return hash;
    }
}
