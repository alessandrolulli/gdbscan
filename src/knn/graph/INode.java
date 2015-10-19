package knn.graph;

import java.io.Serializable;

public interface INode<TID, T> extends Serializable
{
	public TID getId();
	public T getValue();
}
