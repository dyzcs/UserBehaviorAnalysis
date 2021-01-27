package com.dyzcs.hotitems_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * Created by Administrator on 2021/1/27.
 */

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        // 定义事件时间语义
        // flink 1.12 之后默认不用设置
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
        val inputStream = env.readTextFile("UserBehavior.csv")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000L)

        // 得到窗口聚合结果
        val aggStream = dataStream.filter(_.behavior == "pv")
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new ItemViewWindowResult())

        env.execute("hot items")
    }
}

// 自定义预聚合函数AggregateFunction
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
    override def createAccumulator(): Long = 0L

    // 每来一条数据调用一次add，count+1
    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(a: Long, b: Long): Long = a + b
}

class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
        val itemId = key.asInstanceOf[Tuple1[Long]].f0
        val windowEnd = window.getEnd
        val count = input.iterator.next()
        out.collect(ItemViewCount(itemId, windowEnd, count))
    }
}