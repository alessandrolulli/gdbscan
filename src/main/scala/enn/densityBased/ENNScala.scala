package enn.densityBased

import org.apache.spark.SparkContext._
import knn.graph.INode
import knn.graph.NeighborList
import java.util.HashSet
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import org.apache.spark.api.java.function.PairFlatMapFunction
import java.util.Random
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import knn.graph.NeighborListFactory
import knn.graph.Neighbor
import org.apache.spark.storage.StorageLevel
import knn.metric.IMetric

/**
  * @author alemare
  */
class ENNScala[TID : ClassTag, T : ClassTag, TN <: INode[TID, T] : ClassTag](@transient val sc : SparkContext,
                                                                             val _metric : IMetric[TID, T, TN],
                                                                             val _config : ENNConfig)
        extends Serializable {
    val DEFAULT_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK

    def computeGraph(graphInput_ : RDD[(TN, NeighborList[TID, T, TN])],
                     eNNgraphInput_ : RDD[(TN, HashSet[TID])],
                     printer_ : ENNPrintToFile,
                     iterSum_ : Int,
                     nodeManager_ : ENNNodeManager[TID, T, TN]) : RDD[(TN, HashSet[TID])] =
        {
            val neighborListFactory = _metric.getNeighborListFactory();

            var graphENN = eNNgraphInput_
            var graphKNN : RDD[(TN, NeighborList[TID, T, TN])] = graphInput_

            val totalNodes = graphInput_.count

            var nodeExcluded = sc.broadcast(Set[TID]())

            var iteration = 1
            var earlyTermination = false

                // TODO this must be handled by a config variable
                //        val pruning = (_config.k + (Math.max(_config.k, _config.kMax) - _config.k) * 2)

                def computingNode(id_ : TID, iterationNumber_ : Int, splitComputationNumber_ : Int) : Boolean =
                    {
                        id_.hashCode() % splitComputationNumber_ == iterationNumber_ % splitComputationNumber_
                    }

            while (iteration <= _config.maxIterations && !earlyTermination) //        for ( iteration <- 1 to _config.maxIterations )
            {
                val timeBegin = System.currentTimeMillis()
                val iterationNumber = iteration + iterSum_;
                var messageNumber = 0L

                /**
                  * some statistics and data to alternate the computation
                  * these are not mandatory. If alternating computation is not required can be avoided
                  */
                val activeNodes = graphKNN.count

                val splitComputationNumber = Math.max(Math.ceil(activeNodes / _config.maxComputingNodes.toDouble).toInt, 1)

                /**
                  * from directed graph to undirected
                  */
                val graphKNNiterationStart = graphKNN
                //            : RDD[(TN, Iterable[TN])]

                val exploded_graph =
                    graphKNN.flatMap(tuple => tuple._2
                        .flatMap(t =>
                            {
                                if (computingNode(t.node.getId, iterationNumber, splitComputationNumber)) {
                                    Array((tuple._1, t.node), (t.node, tuple._1))
                                }
                                else {
                                    Array((tuple._1, t.node))
                                }
                            })).groupByKey // i obtained equals performance
                //.reduceByKey((a,b) => a++b)
                exploded_graph.persist(DEFAULT_STORAGE_LEVEL).first();

                if (_config.messageStat) {
                    messageNumber += exploded_graph.count
                }

                /**
                  * some statistics for plotting
                  */
                val computingNodes = if (_config.performance) 0 else exploded_graph.filter(tuple => tuple._1.getId.hashCode() % splitComputationNumber == iterationNumber % splitComputationNumber).count

                    /**
                      * each node calculate all pairs similarity between neighbors and
                      * send to each neighbor the list of all the neighbors paired with the similarities
                      */
                    def neighborSimilarityInnerLoop(node : TN,
                                                    list : Iterable[TN],
                                                    nodeManager : ENNNodeManager[TID, T, TN],
                                                    nodeExcluded : Broadcast[Set[TID]],
                                                    neighborListFactory : NeighborListFactory[TID, T, TN],
                                                    self : Boolean) : NeighborList[TID, T, TN] =
                        {
                            val nl = neighborListFactory.create(_config.k);

                            val tValue = nodeManager.getNodeValue(node)

                            if (tValue.isDefined && !nodeExcluded.value.contains(node.getId)) {
                                list.map(u =>
                                    {
                                        val uValue = nodeManager.getNodeValue(u)
                                        if (u != node && uValue.isDefined && !nodeExcluded.value.contains(u.getId)) {
                                            nl.add(new Neighbor[TID, T, TN](u, _metric.compare(uValue.get, tValue.get)))
                                        }
                                    })
                            }

                            nl
                        }

                    def neighborSimilarityLoopAll(nodes : Set[TN],
                                                  nodeManager : ENNNodeManager[TID, T, TN],
                                                  nodeExcluded : Broadcast[Set[TID]],
                                                  neighborListFactory : NeighborListFactory[TID, T, TN]) : Iterable[(TN, NeighborList[TID, T, TN])] =
                        {
                            val toReturn : Map[TID, (TN, NeighborList[TID, T, TN])] = nodes.map(t => (t.getId, (t, neighborListFactory.create(_config.k)))).toMap

                            nodes.map(t =>
                                {
                                    val tValue = nodeManager.getNodeValue(t)

                                    if (tValue.isDefined && !nodeExcluded.value.contains(t.getId)) {
                                        nodes.map(u =>
                                            {
                                                if (u.getId.hashCode < t.getId.hashCode && !nodeExcluded.value.contains(u.getId)) {
                                                    val uValue = nodeManager.getNodeValue(u)
                                                    if(uValue.isDefined)
                                                    {
                                                        val r = _metric.compare(uValue.get, tValue.get)
                                                        toReturn(u.getId)._2.add(new Neighbor[TID, T, TN](t, r))
                                                        toReturn(t.getId)._2.add(new Neighbor[TID, T, TN](u, r))
                                                    }
                                                }
                                            })
                                    }

                                })
                            toReturn.map(t => (t._2._1, t._2._2)).toIterable
                        }

                    def samplingNodes(self_ : TN, list_ : Iterable[TN]) =
                        {
                            if (_config.sampling > 0) {
                                list_.toSet.take((_config.k * _config.sampling) - 1) ++ Set(self_)
                            }
                            else {
                                list_.toSet ++ Set(self_)
                            }
                        }

                graphKNN = exploded_graph.flatMap(tuple =>
                    {
                        /**
                          * alternating computation
                          * in iteration t only the nodes having id % t equals to 0 perform all pairs similarity of the neighbors
                          */
                        if (computingNode(tuple._1.getId, iterationNumber, splitComputationNumber)) {
                            val nodes = samplingNodes(tuple._1, tuple._2)

                            if (_config.knnMetricDoubleCalculation) {
                                nodes.map(t => (t, neighborSimilarityInnerLoop(t, nodes, nodeManager_, nodeExcluded, neighborListFactory, t.getId == tuple._1.getId)))
                            }
                            else {
                                neighborSimilarityLoopAll(nodes, nodeManager_, nodeExcluded, neighborListFactory)
                            }

                        }
                        else {
                            Array[(TN, NeighborList[TID, T, TN])]((tuple._1, neighborSimilarityInnerLoop(tuple._1, tuple._2, nodeManager_, nodeExcluded, neighborListFactory, true)))
                        }
                    }).filter(t => !t._2.isEmpty()).persist(DEFAULT_STORAGE_LEVEL)

                graphKNN.persist(DEFAULT_STORAGE_LEVEL).first();
                exploded_graph.unpersist();
                graphKNNiterationStart.unpersist();

                if (_config.messageStat) {
                    messageNumber += graphKNN.count
                }

                /**
                  * for each node we gather the better kMax neighbors (in reduceByKey)
                  * after (in map) we generate an rdd where for each node we keep:
                  * 1) the best k neighbors -> for the KNN graph
                  * 2) all the neighbors valid for ENN -> for the ENN graph
                  */
                val graphBeforeFilter : RDD[(TN, (NeighborList[TID, T, TN], HashSet[TID]))] = graphKNN.reduceByKey((a, b) =>
                    {
                        val neighborKNN = neighborListFactory.create(Math.max(_config.k, _config.kMax))
                        val neighborKNNSlim = neighborListFactory.create(_config.k)

                        def addDataToList(t : Neighbor[TID, T, TN]) =
                            {
                                if (_metric.isValid(t, _config.epsilon)) {
                                    neighborKNN.add(t)
                                }
                                else {
                                    neighborKNNSlim.add(t)
                                }
                            }

                        a.map(addDataToList)
                        b.map(addDataToList)

                        if (neighborKNN.size >= _config.k) {
                            neighborKNN
                        }
                        else {
                            neighborKNN.addAll(neighborKNNSlim)
                            neighborKNN
                        }
                    }).map(t => (t._1, (
                    {
                        val neighborKNN = neighborListFactory.create(_config.k)
                        neighborKNN.addAll(t._2)

                        neighborKNN
                    },
                    {
                        val neighborENN = new HashSet[TID]();

                        t._2.map(u =>
                            {
                                if (_metric.isValid(u, _config.epsilon)) {
                                    neighborENN.add(u.node.getId());
                                }
                            })

                        neighborENN
                    })))

                /**
                  * for what concern the KNN construction we keep only the top k neighbors and we discard the ENN neighbors
                  */
                graphBeforeFilter.persist(DEFAULT_STORAGE_LEVEL).first
                val previousGraphKNN = graphKNN
                graphKNN = graphBeforeFilter.map(t => (t._1, t._2._1))

                /**
                  * for what concern the ENN construction we keep only the ENN neighbors and we discard the KNN neighbors
                  * than we join the previous ENN with the current ENN neighbors
                  * it returns also a boolean meaning that the vertex must be excluded or not from the computation
                  */
                val previousGraphENN = graphENN

                if (_config.messageStat) {
                    // to count both message directed to KNN and to ENN
                    messageNumber += graphKNN.count * 2
                }

                val graphENNPlusExclusion : RDD[(TN, (HashSet[TID], Boolean))] =
                    graphENN.leftOuterJoin(graphBeforeFilter.map(t => (t._1, t._2._2))).map(arg =>
                        {
                            if (arg._2._1.size() >= _config.kMax) {
                                (arg._1, (arg._2._1, arg._2._2.isDefined))
                            }
                            else {
                                if (arg._2._2.isDefined) {
                                    val nl = new HashSet[TID]();

                                    nl.addAll(arg._2._1);
                                    nl.addAll(arg._2._2.get);

                                    (arg._1, (nl, nl.size() >= _config.kMax))
                                }
                                else {
                                    (arg._1, (arg._2._1, arg._2._1.size() >= _config.kMax))
                                }
                            }
                        })

                val nodeExludedNumber = nodeExcluded.value.size
                
                if(! _config.instrumented)
                {
                  nodeExcluded = sc.broadcast(graphENNPlusExclusion.filter(t => t._2._2).map(t => t._1.getId).collect().toSet)
                }
                
                graphENN = graphENNPlusExclusion.map(t => (t._1, t._2._1)).persist(DEFAULT_STORAGE_LEVEL)

                graphENN.first();
                previousGraphENN.unpersist()
                graphENNPlusExclusion.unpersist()
                graphKNN.persist(DEFAULT_STORAGE_LEVEL).first
                previousGraphKNN.unpersist()
                graphBeforeFilter.unpersist()

                val timeEnd = System.currentTimeMillis()

                if (!_config.performance) {
                    printer_.printENN[TID, T, TN](graphENN, iterationNumber);
                }
                if(_config.instrumented)
                {
                  printer_.printKNNDistribution(iteration, graphKNN)
                }

                printer_.printTime(iterationNumber, timeEnd - timeBegin, activeNodes, computingNodes, nodeExludedNumber, messageNumber)

                /*
             * early termination mechanism
             */
                if (activeNodes < (totalNodes * _config.terminationActiveNodes) && nodeExludedNumber < (totalNodes * _config.terminationRemovedNodes)) {
                    earlyTermination = true
                }
                iteration = iteration + 1
            }
            
            if(_config.instrumented)
            {
              printer_.printENNDistribution[TID, T, TN](iteration, graphENN)
            }

            graphENN
        }

    def initializeENN(nodes_ : RDD[TN]) =
        {
            nodes_.map(t => (t, new HashSet[TID]()))
        }

    def initializeAndComputeGraph(nodes_ : RDD[TN], nodeManager : ENNNodeManager[TID, T, TN], partitionNumber_ : Int, printer_ : ENNPrintToFile) : RDD[(TN, HashSet[TID])] =
        {
            val eNNgraph : RDD[(TN, HashSet[TID])] = initializeENN(nodes_)

            initializeAndComputeGraphWithENN(nodes_, nodeManager, partitionNumber_, printer_, 0, eNNgraph);
        }

    def initializeAndComputeGraphWithENN(nodes_ : RDD[TN],
                                         nodeManager : ENNNodeManager[TID, T, TN],
                                         partitionNumber_ : Int,
                                         printer_ : ENNPrintToFile,
                                         iterSum_ : Int,
                                         eNNgraph_ : RDD[(TN, HashSet[TID])]) : RDD[(TN, HashSet[TID])] =
        {
            /*
         * each node is sent to 10 random "buckets"
         */
            val randomized : RDD[(Int, TN)] = nodes_.flatMap(n =>
                {
                    /*
            * rand must be initialized here
            * if put outside the algorithm works worst (!!!???)
            */
                    val rand = new Random();
                    (1 to 10) map (_ => (rand.nextInt(partitionNumber_), n))
                })

            /*
         * inside each "bucket", each node take k random neighbors
         */
            val random_nl : RDD[(TN, NeighborList[TID, T, TN])] = randomized.groupByKey.flatMap(tuple =>
                {
                    /*
                     * rand must be initialized here
                     * if put outside the algorithm works worst (!!!???)
                     */
                    val rand = new Random();
                    val nodes = tuple._2.toArray

                    tuple._2.map(t =>
                        {
                            val nnl = new NeighborList[TID, T, TN](_config.k);

                            (1 to _config.k) map (_ => nnl.add((new Neighbor[TID, T, TN](nodes(rand.nextInt(nodes.size)), Double.MaxValue))))

                            (t, nnl)
                        })
                })

            /*
         * each node merge the data from all the 10 different buckets and we have the KNN to start the computation randomly connected
         */
            val graph : RDD[(TN, NeighborList[TID, T, TN])] = random_nl.reduceByKey((a, b) =>
                {
                    val nnl = new NeighborList[TID, T, TN](_config.k);
                    nnl.addAll(a);
                    nnl.addAll(b);

                    nnl
                })

            graph.persist(DEFAULT_STORAGE_LEVEL).count();
            randomized.unpersist();
            random_nl.unpersist();

            computeGraph(graph, eNNgraph_, printer_, iterSum_, nodeManager);
        }
}