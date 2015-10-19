//package enn.densityBased;
//
//import java.io.Serializable;
//import java.security.InvalidParameterException;
//import java.util.ArrayList;
//import java.util.HashSet;
//import java.util.Random;
//
//import knn.graph.IMetric;
//import knn.graph.INode;
//import knn.graph.Neighbor;
//import knn.graph.NeighborList;
//import knn.graph.NeighborListFactory;
//
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFlatMapFunction;
//import org.apache.spark.api.java.function.PairFunction;
//
//import scala.Tuple2;
//
//import com.google.common.base.Optional;
//
//public class ENN<T, TN extends INode<T>> implements Serializable
//{
//	private static final long serialVersionUID = -4757629896486328499L;
//
//	private final int _maxIterations;
//	private final int _k;
//	private final int _kMax;
//	private final double _epsilon;
//	private final IMetric<T, TN> _similarity;
//
//	public ENN(int k_, int kMax_, double epsilon_, int maxIterations_,
//			IMetric<T, TN> similarity_)
//	{
//		_k = k_;
//		_kMax = kMax_;
//		_epsilon = epsilon_;
//		_maxIterations = maxIterations_;
//		_similarity = similarity_;
//	}
//
//	public JavaPairRDD<TN, HashSet<String>> computeGraph(
//			JavaPairRDD<TN, NeighborList<T, TN>> graph,
//			JavaPairRDD<TN, HashSet<String>> eNNgraph, ENNPrintToFile printer_,
//			int iterSum_)
//	{
//		final NeighborListFactory<T, TN> neighborListFactory_ = _similarity
//				.getNeighborListFactory();
//
//		for (int iteration = 0; iteration < _maxIterations; iteration++)
//		{
//			// Reverse
//			final JavaPairRDD<TN, TN> exploded_graph = graph
//					.flatMapToPair(new PairFlatMapFunction<Tuple2<TN, NeighborList<T, TN>>, TN, TN>()
//					{
//
//						@Override
//						public Iterable<Tuple2<TN, TN>> call(
//								Tuple2<TN, NeighborList<T, TN>> tuple)
//								throws Exception
//						{
//
//							final ArrayList<Tuple2<TN, TN>> r = new ArrayList<Tuple2<TN, TN>>();
//
//							for (final Neighbor<T, TN> neighbor : tuple._2())
//							{
//
//								r.add(new Tuple2<TN, TN>(tuple._1(),
//										neighbor.node));
//								r.add(new Tuple2<TN, TN>(neighbor.node, tuple
//										._1()));
//							}
//							return r;
//
//						}
//					});
//
//			exploded_graph.cache().first();
//			graph.unpersist();
//
//			//
//			graph = exploded_graph
//					.groupByKey()
//					.flatMapToPair(
//							new PairFlatMapFunction<Tuple2<TN, Iterable<TN>>, TN, NeighborList<T, TN>>()
//							{
//
//								@Override
//								public Iterable<Tuple2<TN, NeighborList<T, TN>>> call(
//										Tuple2<TN, Iterable<TN>> tuple)
//										throws Exception
//								{
//
//									// Fetch all nodes
//									final ArrayList<TN> nodes = new ArrayList<TN>();
//									nodes.add(tuple._1);
//
//									for (final TN n : tuple._2)
//									{
//										nodes.add(n);
//									}
//
//									//
//									final ArrayList<Tuple2<TN, NeighborList<T, TN>>> r = new ArrayList<Tuple2<TN, NeighborList<T, TN>>>(
//											nodes.size());
//
//									for (final TN n : nodes)
//									{
//										// NeighborList<T, TN> nl =
//										// neighborListFactory_.create(Math.min(_k/2,3));
//										final NeighborList<T, TN> nl = neighborListFactory_
//												.create(_k);
//
//										for (final TN other : nodes)
//										{
//											if (other.equals(n))
//											{
//												continue;
//											}
//
//											nl.add(new Neighbor<T, TN>(other,
//													_similarity.compare(
//															n.getValue(),
//															other.getValue())));
//										}
//
//										r.add(new Tuple2<TN, NeighborList<T, TN>>(
//												n, nl));
//									}
//
//									return r;
//
//								}
//							});
//
//			graph.cache().first();
//
//			// Filter
//			final JavaPairRDD<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>> graphBeforeFilter = graph
//					.groupByKey()
//					.mapToPair(
//							new PairFunction<Tuple2<TN, Iterable<NeighborList<T, TN>>>, TN, Tuple2<NeighborList<T, TN>, HashSet<String>>>()
//							{
//
//								@Override
//								public Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>> call(
//										Tuple2<TN, Iterable<NeighborList<T, TN>>> tuple)
//										throws Exception
//								{
//
//									final NeighborList<T, TN> nl = neighborListFactory_
//											.create(_k);
//									final HashSet<String> nlMax = new HashSet<>();
//
//									for (final NeighborList<T, TN> other : tuple
//											._2())
//									{
//										for (final Neighbor<T, TN> n : other)
//										{
//											// nl.add(n);
//
//											if (_similarity
//													.isValid(n, _epsilon))
//											{
//												nlMax.add(n.node.getId());
//											}
//										}
//										nl.addAll(other);
//									}
//
//									return new Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>>(
//											tuple._1, new Tuple2(nl, nlMax));
//								}
//							});
//
//			graph = graphBeforeFilter
//					.mapToPair(new PairFunction<Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>>, TN, NeighborList<T, TN>>()
//					{
//						@Override
//						public Tuple2<TN, NeighborList<T, TN>> call(
//								Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>> arg)
//								throws Exception
//						{
//							return new Tuple2<TN, NeighborList<T, TN>>(arg._1,
//									arg._2._1);
//						}
//					});
//
//			// eNNgraph = eNNgraph.union(graphBeforeFilter.mapToPair(new
//			// PairFunction<Tuple2<TN,Tuple2<NeighborList<T,
//			// TN>,HashSet<String>>>, TN, HashSet<String>>()
//			// {
//			// @Override
//			// public Tuple2<TN, HashSet<String>> call(
//			// Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>> arg)
//			// throws Exception
//			// {
//			// return new Tuple2<TN, HashSet<String>>(arg._1, arg._2._2);
//			// }
//			// })).groupByKey().mapToPair(new
//			// PairFunction<Tuple2<TN,Iterable<HashSet<String>>>, TN,
//			// HashSet<String>>() {
//			//
//			// @Override
//			// public Tuple2<TN, HashSet<String>> call(
//			// Tuple2<TN, Iterable<HashSet<String>>> arg0)
//			// throws Exception {
//			//
//			// HashSet<String> nl = new HashSet<String>();
//			// for(HashSet<String> i : arg0._2)
//			// {
//			// nl.addAll(i);
//			// }
//			// // TODO Auto-generated method stub
//			// return new Tuple2<TN, HashSet<String>>(arg0._1, nl);
//			// }
//			// }).cache();
//
//			eNNgraph = eNNgraph
//					.leftOuterJoin(
//							graphBeforeFilter
//									.mapToPair(new PairFunction<Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>>, TN, HashSet<String>>()
//									{
//										@Override
//										public Tuple2<TN, HashSet<String>> call(
//												Tuple2<TN, Tuple2<NeighborList<T, TN>, HashSet<String>>> arg)
//												throws Exception
//										{
//											return new Tuple2<TN, HashSet<String>>(
//													arg._1, arg._2._2);
//										}
//									}))
//					.mapToPair(
//							new PairFunction<Tuple2<TN, Tuple2<HashSet<String>, Optional<HashSet<String>>>>, TN, HashSet<String>>()
//							{
//
//								@Override
//								public Tuple2<TN, HashSet<String>> call(
//										Tuple2<TN, Tuple2<HashSet<String>, Optional<HashSet<String>>>> arg)
//										throws Exception
//								{
//
//									// if(arg._2._2.isPresent())
//									// {
//									// HashSet<String> nl = new
//									// HashSet<String>();
//									//
//									// nl.addAll(arg._2._1);
//									// nl.addAll(arg._2._2.get());
//									//
//									// if(nl.size() < _kMax)
//									// {
//									//
//									// return new Tuple2<TN,
//									// HashSet<String>>(arg._1, nl);
//									// } else
//									// {
//									// List<String> list = new
//									// LinkedList<String>(nl);
//									// Collections.shuffle(list);
//									// HashSet<String> randomSet = new
//									// HashSet<String>(list.subList(0, _kMax));
//									//
//									// return new Tuple2<TN,
//									// HashSet<String>>(arg._1, randomSet);
//									// }
//									//
//									// } else
//									// {
//									// return new Tuple2<TN,
//									// HashSet<String>>(arg._1, arg._2._1);
//									// }
//
//									// WORKING WELL FOR CITIES DATASET
//									if (arg._2._1.size() >= _kMax)
//									{
//										return new Tuple2<TN, HashSet<String>>(
//												arg._1, arg._2._1);
//									} else
//									{
//										if (arg._2._2.isPresent())
//										{
//											final HashSet<String> nl = new HashSet<String>();
//
//											nl.addAll(arg._2._1);
//											nl.addAll(arg._2._2.get());
//
//											return new Tuple2<TN, HashSet<String>>(
//													arg._1, nl);
//										} else
//										{
//											return new Tuple2<TN, HashSet<String>>(
//													arg._1, arg._2._1);
//										}
//									}
//
//								}
//							}).cache();
//
//			// new PairFunction<Tuple2<TN,Tuple2<NeighborList<T,
//			// TN>,NeighborList<T, TN>>>, TN, NeighborList<T, TN>>() {
//			//
//			// @Override
//			// public Tuple2<TN, NeighborList<T, TN>> call(
//			// Tuple2<TN, Tuple2<NeighborList<T, TN>, NeighborList<T, TN>>> arg)
//			// throws Exception {
//			//
//			// NeighborList<T, TN> nl = arg._2._1;
//			// nl.addAll(arg._2._2);
//			//
//			// return new Tuple2<TN, NeighborList<T, TN>>(arg._1, nl);
//			// }
//			// }
//
//			eNNgraph.first();
//			graph.cache().first();
//			exploded_graph.unpersist();
//
//			final int iterToPrint = iteration + iterSum_ + 1;
//			printer_.print(eNNgraph, iterToPrint);
//		}
//
//		// return graph;
//		return eNNgraph;
//	}
//
//	public JavaPairRDD<TN, HashSet<String>> initializeAndComputeGraph(
//			JavaRDD<TN> nodes, final int partitionNumber,
//			ENNPrintToFile printer_)
//	{
//		final JavaPairRDD<TN, HashSet<String>> eNNgraph = nodes
//				.mapToPair(new PairFunction<TN, TN, HashSet<String>>()
//				{
//
//					@Override
//					public Tuple2<TN, HashSet<String>> call(TN arg)
//							throws Exception
//					{
//						return new Tuple2<TN, HashSet<String>>(arg,
//								new HashSet<String>());
//					}
//
//				});
//
//		return initializeAndComputeGraphWithENN(nodes, partitionNumber,
//				printer_, 0, eNNgraph);
//	}
//
//	public JavaPairRDD<TN, HashSet<String>> initializeAndComputeGraphWithENN(
//			JavaRDD<TN> nodes, final int partitionNumber,
//			ENNPrintToFile printer_, int iterSum_,
//			JavaPairRDD<TN, HashSet<String>> eNNgraph)
//	{
//		if (_similarity == null)
//		{
//			throw new InvalidParameterException("Similarity is not defined!");
//		}
//
//		final JavaPairRDD<Integer, TN> randomized = nodes
//				.flatMapToPair(new PairFlatMapFunction<TN, Integer, TN>()
//				{
//
//					Random rand = new Random();
//
//					@Override
//					public Iterable<Tuple2<Integer, TN>> call(TN n)
//							throws Exception
//					{
//						final ArrayList<Tuple2<Integer, TN>> r = new ArrayList<Tuple2<Integer, TN>>();
//						for (int i = 0; i < 10; i++)
//						{
//							r.add(new Tuple2<Integer, TN>(rand
//									.nextInt(partitionNumber), n));
//						}
//
//						return r;
//					}
//				});
//
//		// Inside bucket, associate
//		final JavaPairRDD<TN, NeighborList<T, TN>> random_nl = randomized
//				.groupByKey()
//				.flatMapToPair(
//						new PairFlatMapFunction<Tuple2<Integer, Iterable<TN>>, TN, NeighborList<T, TN>>()
//						{
//							Random rand = new Random();
//
//							@Override
//							public Iterable<Tuple2<TN, NeighborList<T, TN>>> call(
//									Tuple2<Integer, Iterable<TN>> tuple)
//									throws Exception
//							{
//
//								final ArrayList<TN> nodes = new ArrayList<TN>();
//								for (final TN n : tuple._2)
//								{
//									nodes.add(n);
//								}
//
//								final ArrayList<Tuple2<TN, NeighborList<T, TN>>> r = new ArrayList<Tuple2<TN, NeighborList<T, TN>>>();
//								for (final TN n : nodes)
//								{
//									final NeighborList<T, TN> nnl = new NeighborList<T, TN>(
//											_k);
//									for (int i = 0; i < _k; i++)
//									{
//										nnl.add(new Neighbor<T, TN>(
//												nodes.get(rand.nextInt(nodes
//														.size())),
//												Double.MAX_VALUE));
//									}
//
//									r.add(new Tuple2<TN, NeighborList<T, TN>>(
//											n, nnl));
//								}
//
//								return r;
//							}
//						});
//
//		// Merge
//		final JavaPairRDD<TN, NeighborList<T, TN>> graph = random_nl
//				.reduceByKey(
//
//				new Function2<NeighborList<T, TN>, NeighborList<T, TN>, NeighborList<T, TN>>()
//				{
//
//					@Override
//					public NeighborList<T, TN> call(NeighborList<T, TN> nl1,
//							NeighborList<T, TN> nl2) throws Exception
//					{
//						final NeighborList<T, TN> nnl = new NeighborList<T, TN>(
//								_k);
//						nnl.addAll(nl1);
//						nnl.addAll(nl2);
//						return nnl;
//					}
//				});
//
//		graph.cache().first();
//		randomized.unpersist();
//		random_nl.unpersist();
//
//		return computeGraph(graph, eNNgraph, printer_, iterSum_);
//	}
//}
