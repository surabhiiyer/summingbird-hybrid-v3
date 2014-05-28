/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package summingbird.proto

import com.twitter.summingbird.batch.BatchID
import com.twitter.summingbird.Options
import com.twitter.summingbird.option.CacheSize
import com.twitter.summingbird.storm.{ StormStore, Storm, Executor, StormExecutionConfig}
import com.twitter.summingbird.storm.option.{FlatMapParallelism, SummerParallelism, SpoutParallelism}
import backtype.storm.{Config => BTConfig}
import com.twitter.scalding.Args
import com.twitter.storehaus.memcache.MemcacheStore
import com.twitter.tormenta.spout.KafkaSpout
import com.twitter.util.Await


object ExeStorm {
  def main(args: Array[String]) {
    Executor(args, StormRunner(_))
  }
}

object StormRunner {
  /**
    * These imports bring the requisite serialization injections, the
    * time extractor and the batcher into implicit scope. This is
    * required for the dependency injection pattern used by the
    * Summingbird Storm platform.
    */
  import Serialization._, ViewCount._
  /**
    * "spout" is a concrete Storm source for Status data. This will
    * act as the initial producer of Status instances in the
    * Summingbird word count job.
    */
  val spout = new KafkaSpout(
    KafkaZkConnectionString,
    KafkaTopic,
    "summingbird-prototype-storm"
  )(bytes => Some(parseView(bytes)))

  /**
    * A MergeableStore is a store that's aware of aggregation and
    * knows how to merge in new (K, V) pairs using a Monoid[V]. The
    * Monoid[Long] used by this particular store is being pulled in
    * from the Monoid companion object in Algebird. (Most trivial
    * Monoid instances will resolve this way.)
    *
    * First, the backing store:
    */
  lazy val viewCountStore =
    MemcacheStore.mergeable[(Long, BatchID), Long](MemcacheStore.defaultClient("memcached", "localhost:11211"), "stormLookCount")

  /**
    * the param to store is by name, so this is still not created created
    * yet
    */
  val storeSupplier: StormStore[Long, Long] = Storm.store(viewCountStore)

  /**
    * This function will be called by the storm runner to request the info
    * of what to run. In local mode it will start up as a
    * separate thread on the local machine, pulling tweets off of the
    * TwitterSpout, generating and aggregating key-value pairs and
    * merging the incremental counts in the memcache store.
    *
    * Before running this code, make sure to start a local memcached
    * instance with "memcached". ("brew install memcached" will get
    * you all set up if you don't already have memcache installed
    * locally.)
    */

  def apply(args: Args): StormExecutionConfig = {
    new StormExecutionConfig {
      override val name = "SummingbirdExample"
      override def transformConfig(config: Map[String, AnyRef]): Map[String, AnyRef] = {
        config ++ List((BTConfig.TOPOLOGY_ACKER_EXECUTORS -> (new java.lang.Integer(0))))
      }
      override def getNamedOptions: Map[String, Options] = Map(
        "DEFAULT" -> Options().set(SummerParallelism(2))
                      .set(FlatMapParallelism(80))
                      .set(SpoutParallelism(16))
                      .set(CacheSize(100))
      )
      override def graph = viewCount[Storm](spout, storeSupplier)
    }
  }
  def lookup(lookId: Long): Option[Long] =
    Await.result {
      viewCountStore.get(lookId -> ViewCount.batcher.currentBatch)
    }
}
