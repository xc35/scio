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

package com.spotify.scio.io

import java.util.UUID

import com.spotify.scio.ScioContext
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.apache.beam.sdk.io.TFRecordIO.CompressionType
import org.apache.commons.io.FileUtils
import org.tensorflow.example._

class TFTapTest extends TapSpec {

  private def getDummyExample = {
    val features = Seq((1.0F, 2.0F), (5.0F, 3.0F))
    features.map { kv =>
      Example.newBuilder().setFeatures(Features.newBuilder()
        .putFeature("f1", Feature.newBuilder()
          .setFloatList(FloatList.newBuilder().addValue(kv._1)).build())
        .putFeature("f2", Feature.newBuilder()
          .setFloatList(FloatList.newBuilder().addValue(kv._2)).build())
      ).build()
    }
  }

  "SCollection" should "support saveAsTFRecordFile" in {
    import com.spotify.scio.tensorflow._
    val data = Seq.fill(100)(UUID.randomUUID().toString)
    import org.apache.beam.sdk.io.TFRecordIO.{CompressionType => CType}
    for (compressionType <- Seq(CType.NONE, CType.ZLIB, CType.GZIP)) {
      val dir = tmpDir
      val t = runWithFileFuture {
        _
          .parallelize(data)
          .map(_.getBytes)
          .saveAsTfRecordFile(dir.getPath, compressionType = compressionType)
      }
      verifyTap(t.map(new String(_)), data.toSet)
      FileUtils.deleteDirectory(dir)
    }
  }

  it should "support saveAsTfExampleFile" in {
    import com.spotify.scio.tensorflow._
    val examples = getDummyExample
    import org.apache.beam.sdk.io.TFRecordIO.{CompressionType => CType}
    for (compressionType <- Seq(CType.NONE, CType.ZLIB, CType.GZIP)) {
      val dir = tmpDir
      val sc = ScioContext()
      val featureSpec = sc.parallelize(Seq(Seq("f1", "f2")))
      val (out, spec) = sc.parallelize(examples)
          .saveAsTfExampleFile(dir.getPath, featureSpec, compressionType = compressionType)
      sc.close().waitUntilDone()
      verifyTap(out.waitForResult(), examples.toSet)
      verifyTap(spec.waitForResult(), Set("f1", "f2"))
      FileUtils.deleteDirectory(dir)
    }

  }

  it should "throw on miltiple feature specifications" in {
    import com.spotify.scio.tensorflow._
    val examples = getDummyExample
    val dir = tmpDir
    // using my own ScioContext cause error is thrown during the pipeline execution not, not DAG
    // construction. Also don't want to specify expected in/out.
    val sc = ScioContext()
    // scalastyle:off no.whitespace.before.left.bracket
    // scalastyle:off line.size.limit
    the [PipelineExecutionException] thrownBy {
      val featureSpec = sc.parallelize(Seq(Seq("f1", "f2"), Seq("f1", "f3")))
      sc.parallelize(examples)
        .saveAsTfExampleFile(dir.getPath, featureSpec, compressionType = CompressionType.NONE)
      sc.close()
    } should have message s"java.lang.IllegalArgumentException: requirement failed: Feature specification must contain a single element"
    // scalastyle:on no.whitespace.before.left.bracket
    // scalastyle:off line.size.limit
    FileUtils.deleteDirectory(dir)
  }

}
