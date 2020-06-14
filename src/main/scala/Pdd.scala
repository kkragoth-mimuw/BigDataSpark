package com.pszulc

import scala.util.Random
import scala.math.min

import org.apache.spark.sql.SparkSession

object Pdd {
    val POINT_TYPE = 0
    val QUERY_TYPE = 1

    def run2d(sc : org.apache.spark.SparkContext, path : String) {
        val rdd = sc.textFile(path)
            .map(r => r.split(","))
            .map(r => ((r(0).toInt, r(1).toInt), r(2).toInt))

        val rddCount = sc.broadcast(rdd.count())

        val sorted1d = rdd.sortBy { case ((dim1, dim2), pointType) =>
                (dim1, pointType)
            }
            .zipWithIndex
            .map { case (((dim1, dim2), pointType), i) => {
                val padWithZerosLength = (rddCount.value - 1).toBinaryString.length - i.toBinaryString.length
                val generatedLabel = "0" * (padWithZerosLength) + i.toBinaryString
                
                (generatedLabel, (dim1, dim2), pointType)
            }}
            .flatMap { case (label, (dim1, dim2), pointType) => {
                val desiredPrefixEnding = pointType match {
                    case POINT_TYPE => '0'
                    case QUERY_TYPE => '1'
                }
                
                val indxs = label.zipWithIndex.filter(_._1 == desiredPrefixEnding).map(_._2)
                
                var createdPrefixes = indxs.map(i => (label.take(i), ((dim1, dim2), pointType)))
                
                if (pointType == QUERY_TYPE && label == ("0" * (rddCount.value - 1).toBinaryString.length)) {
                    createdPrefixes = createdPrefixes :+ (("0" * (rddCount.value - 1).toBinaryString.length), ((dim1, dim2), pointType))
                }
                
                createdPrefixes
            }}
            .sortBy(r => (r._1.length, r._1, r._2._1._2, r._2._2))
            .cache()

        val lastValues = sorted1d
            .mapPartitionsWithIndex((index, it) => {
                val (iter, iterDup) = it.duplicate
                
                var lastPrefix = iterDup.toList.last._1
                var lastCount = iter.filter(p => p._1 == lastPrefix && p._2._2 == POINT_TYPE).length
                
                Iterator((index, lastPrefix, lastCount))
            
            }).collect()
            

        val broadcastLastValues = sc.broadcast(lastValues)

        val queryCounts = sorted1d
            .mapPartitionsWithIndex((index, it) => {
                var queryOutputs = Array[((Int, Int), Int)]()
                
                val firstElem = it.next()
                val firstElemLabel = firstElem._1
                
                val previousServerInfo = broadcastLastValues.value.filter(r => r._1 < index && r._2 == firstElemLabel)
                
                val firstElemCountFromPreviousServer = previousServerInfo.length match { 
                    case 0 => 0
                    case _ => previousServerInfo.map(_._3).reduceLeft[Int](_+_)
                }
                
                var currentLabel = firstElemLabel
                
                var currentCount = firstElem._2._2 match {
                    case POINT_TYPE => firstElemCountFromPreviousServer + 1
                    case QUERY_TYPE => firstElemCountFromPreviousServer
                }
                
                if (firstElem._2._2 == QUERY_TYPE) {
                    queryOutputs = queryOutputs :+ (firstElem._2._1, firstElemCountFromPreviousServer)
                }
                
                for ((label, ((dim1, dim2), pointType)) <- it) {
                    if (label != currentLabel) {
                        currentLabel = label
                        currentCount = 0
                    }
                    pointType match {
                        case POINT_TYPE => currentCount += 1
                        case QUERY_TYPE => {
                            queryOutputs = queryOutputs :+ ((dim1, dim2), currentCount)
                        }
                    }
                }
                
                queryOutputs.toIterator
            })
            .reduceByKey(_+_)
            .collect()

        for (elem <- queryCounts)
            print(elem)
    }

    def run3d(sc : org.apache.spark.SparkContext, path : String) {
        val rdd = sc.textFile(path)
            .map(r => r.split(","))
            .map(r => ((r(0).toInt, r(1).toInt, r(2).toInt), r(3).toInt))

        val rddCount = sc.broadcast(rdd.count())

        val sorted2d =  rdd.sortBy { case ((dim1, dim2, dim3), pointType) =>
                (dim1, pointType)
            }
            .zipWithIndex
            .map { case (((dim1, dim2, dim3), pointType), i) => {
                val padWithZerosLength = (rddCount.value - 1).toBinaryString.length - i.toBinaryString.length
                val generatedLabel = "0" * (padWithZerosLength) + i.toBinaryString
                
                (generatedLabel, (dim1, dim2, dim3), pointType)
            }}
            .flatMap { case (label, (dim1, dim2, dim3), pointType) => {
                val desiredPrefixEnding = pointType match {
                    case POINT_TYPE => '0'
                    case QUERY_TYPE => '1'
                }
                
                val indxs = label.zipWithIndex.filter(_._1 == desiredPrefixEnding).map(_._2)
                var createdPrefixes = indxs.map(i => (label.take(i), ((dim1, dim2, dim3), pointType)))
                
                if (pointType == QUERY_TYPE && label == ("0" * (rddCount.value - 1).toBinaryString.length)) {
                    createdPrefixes = createdPrefixes :+ (("0" * (rddCount.value - 1).toBinaryString.length), ((dim1, dim2, dim3), pointType))
                }
                
                createdPrefixes
            }}
            .sortBy { case (label, ((dim1, dim2, dim3), pointType)) =>
                (label.length, label, dim2, pointType)
            }
            .cache
            
        val sorted2dLabelCountsMap = sorted2d
            .countByKey

        val broadcastSorted2dLabelCountsMap = sc.broadcast(sorted2dLabelCountsMap)

        val sorted2dMinIndexMap = sorted2d
            .zipWithIndex
            .map { case ((label, ((dim1, dim2, dim3), pointType)), i) => 
                (label, i)
            }
            .reduceByKey { case (x, y) => min(x,y) }
            .collect()
            .toMap
            
        val broadcastSorted2dMinIndexMap = sc.broadcast(sorted2dMinIndexMap)

        val sorted1d = sorted2d
            .zipWithIndex
            .map { case ((label, ((dim1, dim2, dim3), pointType)), i) =>
                val n = broadcastSorted2dLabelCountsMap.value(label)
                val indexOfLabel = i - broadcastSorted2dMinIndexMap.value(label)
                
                val padWithZerosLength = (n - 1).toBinaryString.length - indexOfLabel.toBinaryString.length
                val generatedLabel = "0" * (padWithZerosLength) + indexOfLabel.toBinaryString
                
                ((label, generatedLabel), ((dim1, dim2, dim3), pointType))
            }
            .flatMap { case ((label1, label2), ((dim1,dim2,dim3), pointType)) => {
                val desiredPrefixEnding = pointType match {
                    case POINT_TYPE => '0'
                    case QUERY_TYPE => '1'
                }
                
                val indxs = label2.zipWithIndex.filter(_._1 == desiredPrefixEnding).map(_._2)
                var createdPrefixes = indxs.map(i => ((label1, label2.take(i)), ((dim1, dim2, dim3), pointType)))
                
                val zeroLabel = "0" * (broadcastSorted2dLabelCountsMap.value(label1) - 1).toBinaryString.length
                if (pointType == QUERY_TYPE && label2 == zeroLabel) {
                    createdPrefixes = createdPrefixes :+ ((label1, zeroLabel), ((dim1, dim2, dim3), pointType))
                }
                createdPrefixes
            }}
            .sortBy { case ((firstLabel, secondLabel), ((dim1, dim2, dim3), pointType)) => 
                (firstLabel.length, firstLabel, secondLabel.length, secondLabel, dim3, pointType)
            }
            .cache()

        val lastValues = sorted1d
            .mapPartitionsWithIndex((index, it) => {
                val (iter, iterDup) = it.duplicate
                
                var lastPrefix = iterDup.toList.last._1
                var lastCount = iter.filter(p => p._1 == lastPrefix && p._2._2 == POINT_TYPE).length
                
                Iterator((index, lastPrefix, lastCount))
            
            }).collect()
            

        val broadcastLastValues = sc.broadcast(lastValues)

        val queryCounts = sorted1d
            .mapPartitionsWithIndex((index, it) => {
                var queryOutputs = Array[((Int, Int, Int), Int)]()
                
                val firstElem = it.next()
                val firstElemLabel = firstElem._1
                
                val previousServerInfo = broadcastLastValues.value.filter(r => r._1 < index && r._2 == firstElemLabel)
                
                val firstElemCountFromPreviousServer = previousServerInfo.length match { 
                    case 0 => 0
                    case _ => previousServerInfo.map(_._3).reduceLeft[Int](_+_)
                }
                
                var currentLabel = firstElemLabel
                
                var currentCount = firstElem._2._2 match {
                    case POINT_TYPE => firstElemCountFromPreviousServer + 1
                    case QUERY_TYPE => firstElemCountFromPreviousServer
                }
                
                if (firstElem._2._2 == QUERY_TYPE) {
                    queryOutputs = queryOutputs :+ (firstElem._2._1, firstElemCountFromPreviousServer)
                }
                
                
                for ((label, (dims, pointType)) <- it) {
                    if (label != currentLabel) {
                        currentLabel = label
                        currentCount = 0
                    }
                    pointType match {
                        case POINT_TYPE => currentCount += 1
                        case QUERY_TYPE => {
                            queryOutputs = queryOutputs :+ (dims, currentCount)
                        }
                    }
                }
                
                queryOutputs.toIterator
            })
            .reduceByKey(_+_)
            .collect()

            for (elem <- queryCounts)
                print(elem)
    }

    def main(args: Array[String]) {
        val spark = SparkSession
            .builder()
            .appName("pdd")
            .getOrCreate()
            
        val sc = spark.sparkContext
        
        if (args.length < 1) {
            println("Args 0: path, 1: (optional) num_partitions")
            System.exit(0)
        }
        
        val path = args(0)
        val parallelizeCount = if (args.length > 1) args(1).toInt else 3

        val rowSize = sc.textFile(path).first().split(",").length

        if (rowSize == 3) {
            run2d(sc, path)
        }
        else if (rowSize == 4) {
            run3d(sc, path)
        }
        else {
            println("unsupported file type")
        }
}}