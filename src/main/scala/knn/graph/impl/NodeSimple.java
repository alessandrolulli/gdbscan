/*
 * Copyright (C) 2011-2012 the original author or authors.
 * See the LICENCE.txt file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package knn.graph.impl;

import knn.graph.INode;

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

        if (! (other instanceof NodeSimple)) {
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
