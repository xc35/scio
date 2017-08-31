/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.UUID

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.Dataflow
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Seconds}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

object ScioBenchmark {

  private val projectId: String = "scio-playground"

  private val commonArgs = Array(
    s"--project=$projectId",
    "--runner=DataflowRunner",
    "--numWorkers=4",
    "--workerMachineType=n1-standard-4",
    "--autoscalingAlgorithm=NONE")

  private val shuffleArgs = Array("--experiments=shuffle_mode=service")

  private val dataflow = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential.getApplicationDefault
    new Dataflow.Builder(transport, jackson, credential).build()
  }

  def main(args: Array[String]): Unit = {
    val jobNamePrefix = trimName(this.getClass.getSimpleName)
    val timestamp = DateTimeFormat.forPattern("MMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val username = sys.props("user.name")

    val results = benchmarks.map { b =>
      val (sc, _) = ContextAndArgs(commonArgs ++ b.extraArgs)
      val appName = trimName(b.getClass.getCanonicalName)
      val simpleName = trimName(b.getClass.getSimpleName)
      sc.setAppName(appName)
      sc.setJobName(s"$jobNamePrefix-$timestamp-$simpleName-$username".toLowerCase())

      b.run(sc)
      val result = sc.close()
      (b, result)
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future.sequence(results.map(_._2.finalState))
    Await.result(future, Duration.Inf)

    // scalastyle:off regex
    results.foreach { case (benchmark, result) =>
      println("=" * 80)
      prettyPrint("Benchmark", trimName(benchmark.getClass.getSimpleName))
      prettyPrint("Description", benchmark.description)
      prettyPrint("Extra arguments", benchmark.extraArgs.mkString(" "))
      prettyPrint("State", result.state.toString)

      val jobId = result.internal.asInstanceOf[DataflowPipelineJob].getJobId
      val job = dataflow.projects().jobs().get(projectId, jobId).setView("JOB_VIEW_ALL").execute()
      val parser = ISODateTimeFormat.dateTimeParser()
      prettyPrint("Create time", job.getCreateTime)
      prettyPrint("Finish time", job.getCurrentStateTime)
      val start = parser.parseLocalDateTime(job.getCreateTime)
      val finish = parser.parseLocalDateTime(job.getCurrentStateTime)
      val elapsed = PeriodFormat.getDefault.print(Seconds.secondsBetween(start, finish))
      prettyPrint("Elapsed", elapsed)

      result.getMetrics.cloudMetrics
        .filter(m => m.name.name.startsWith("Total") && !m.name.context.contains("tentative"))
        .map(m => (m.name.name, m.scalar.toString))
        .toSeq.sortBy(_._1)
        .foreach(kv => prettyPrint(kv._1, kv._2))
    }
    // scalastyle:on regex
  }

  private def prettyPrint(k: String, v: String): Unit = {
    // scalastyle:off regex
    println("%-20s: %s".format(k, v))
    // scalastyle:on regex
  }

  private def trimName(name: String): String = name.replaceAll("\\$$", "")

  // =======================================================================
  // Benchmarks
  // =======================================================================

  private val benchmarks = Seq(
    GroupByKey,
    GroupByKeyShuffle,
    GroupAll,
    GroupAllShuffle,
    Join,
    JoinShuffle,
    HashJoin,
    SingletonSideInput,
    IterableSideInput,
    ListSideInput,
    MapSideInput,
    MultiMapSideInput)

  abstract class Benchmark(val description: String, val extraArgs: Array[String] = Array.empty) {
    def run(sc: ScioContext): Unit
  }

  // ===== GroupByKey =====

  // 100M items, 10K keys, average 10K values per key
  object GroupByKey extends Benchmark("GBK") {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => Random.nextInt(10 * K)).mapValues(_.size)
  }

  object GroupByKeyShuffle extends Benchmark("GBK with Shuffle Service", shuffleArgs) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => Random.nextInt(10 * K)).mapValues(_.size)
  }

  // 100M items, 1 key
  object GroupAll extends Benchmark("GroupAll") {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => 0).mapValues(_.size)
  }

  object GroupAllShuffle extends Benchmark("GroupAll with Shuffle Service", shuffleArgs) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => 0).mapValues(_.size)
  }

  // ===== Join =====

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 50M items, 5M keys, average 10 values per key
  object Join extends Benchmark("Join") {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) join randomKVs(sc, 50 * M, 5 * M)
  }

  object JoinShuffle extends Benchmark("Join with Shuffle Service", shuffleArgs) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) join randomKVs(sc, 50 * M, 5 * M)
  }

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 1M items, 100K keys, average 10 values per key
  object HashJoin extends Benchmark("HashJoin") {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) hashJoin randomKVs(sc, M, 100 * K)
  }

  // ===== SideInput =====

  // Main: 100M, side: 1M

  object SingletonSideInput extends Benchmark("SingletonSideInput") {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).map(Set(_)).sum.asSingletonSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).size) }
    }
  }

  object IterableSideInput extends Benchmark("IterableSideInput") {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asIterableSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).size) }
    }
  }

  object ListSideInput extends Benchmark("ListSideInput") {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asListSideInput
      main.withSideInputs(side)
        .map { case (x, s) => (x, s(side).size) }
    }
  }

  object MapSideInput extends Benchmark("MapSideInput") {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = main
        .sample(withReplacement = false, 0.01)
        .map((_, UUID.randomUUID().toString))
        .asMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  object MultiMapSideInput extends Benchmark("MultiMapSideInput") {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 1 * M)
      val side = main
        .sample(withReplacement = false, 0.01)
        .map((_, UUID.randomUUID().toString))
        .asMultiMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  // =======================================================================
  // Utilities
  // =======================================================================

  private val M = 1000000
  private val K = 1000

  private def randomUUIDs(sc: ScioContext, n: Long): SCollection[String] =
    sc.parallelize(Seq.fill(100)(n / 100))
      .applyTransform(ParDo.of(new FillDoFn(() => UUID.randomUUID().toString)))

  private def randomKVs(sc: ScioContext,
                        n: Long, numUniqueKeys: Int): SCollection[(String, String)] =
    sc.parallelize(Seq.fill(100)(n / 100))
      .applyTransform(ParDo.of(new FillDoFn(() =>
        ("key" + Random.nextInt(numUniqueKeys), UUID.randomUUID().toString)
      )))

  private class FillDoFn[T](val f: () => T) extends DoFn[Long, T] {
    @ProcessElement
    def processElement(c: DoFn[Long, T]#ProcessContext): Unit = {
      var i = 0L
      val n = c.element()
      while (i < n) {
        c.output(f())
        i += 1
      }
    }
  }
}
