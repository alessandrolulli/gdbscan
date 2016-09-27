package util.converter;

import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

public class EdgelistFromAlphabetToNumber
{
	private static final AtomicInteger _incrementalId = new AtomicInteger(0);
	private static final BiMap<String, Integer> _vertexMapping = HashBiMap.create();

	public static void main(final String[] args_)
	{
		// 0 -> input
		// 1 -> output

		try
		{
			final FileOutputStream fileEdgeList = new FileOutputStream(args_[1]);
			final PrintStream printEdgeList = new PrintStream(fileEdgeList);

			final FileReader fr = new FileReader(args_[0]);
			final LineNumberReader lnr = new LineNumberReader(fr);
			String line;
			boolean skipFirstLine = true;

			while ((line = lnr.readLine()) != null)
			{
				if(skipFirstLine)
				{
					skipFirstLine = false;
					continue;
				}
				
				if (line.startsWith("#"))
				{
					continue;
				}
				final StringTokenizer st = new StringTokenizer(line);

				if (!st.hasMoreTokens() || (st.countTokens() < 2))
				{
					continue;
				}

				final int vertexA = getVertexId(st.nextToken());
				final int vertexB = getVertexId(st.nextToken());

				printEdgeList.println(vertexA+" "+vertexB);
			}

			printEdgeList.flush();
			printEdgeList.close();
			lnr.close();
		} catch (final IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	public static int getVertexId(final String val_)
	{
		final Optional<Integer> id = Optional.fromNullable(_vertexMapping.get(val_));

		if(id.isPresent())
		{
			return id.get();
		} else
		{
			final int nextId = _incrementalId.getAndIncrement();
			_vertexMapping.put(val_, nextId);

			return nextId;
		}
	}
}
