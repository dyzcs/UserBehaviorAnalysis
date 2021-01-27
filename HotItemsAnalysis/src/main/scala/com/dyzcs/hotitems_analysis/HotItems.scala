package com.dyzcs.hotitems_analysis

import org.apache.flink.streaming.api.scala._

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
        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
        val inputStream = env.readTextFile("UserBehavior.csv")
        val dataStream = inputStream.map(data => {
            val arr = data.split(",")
            UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
        }).assignAscendingTimestamps(_.timestamp * 1000L)

        env.execute("hot items")
    }
}
