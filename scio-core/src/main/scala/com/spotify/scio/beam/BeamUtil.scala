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

package com.spotify.scio.beam

import com.spotify.scio.util.ClosureCleaner
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, SerializableFunction}

object BeamUtil {

  def serializableFunction[A]: SerializableFunctionBuilder[A] = new SerializableFunctionBuilder[A]

  def doFn[A, B]: DoFnBuilder[A, B] = new DoFnBuilder[A, B]

  class SerializableFunctionBuilder[A] private[beam] () {
    def apply[B](f: A => B): SerializableFunction[A, B] = new SerializableFunction[A, B] {
      val g = ClosureCleaner(f)  // defeat closure
      override def apply(input: A): B = g(input)
    }
  }

  class DoFnBuilder[A, B] private[beam] () {
    private var hasSetup = false
    private var hasTeardown = false
    private var hasStartBundle = false
    private var hasFinishBundle = false

    private var setupFn: () => Unit = () => Unit
    private var teardownFn: () => Unit = () => Unit
    private var startBundleFn: DoFn[A, B]#StartBundleContext => Unit = (_) => Unit
    private var finishBundleFn: DoFn[A, B]#FinishBundleContext => Unit = (_) => Unit

    def withSetup(f: () => Unit): DoFnBuilder[A, B] = {
      require(!hasSetup, "Setup already defined")
      hasSetup = true
      setupFn = f
      this
    }

    def withTeardown(f: () => Unit): DoFnBuilder[A, B] = {
      require(!hasTeardown, "Teardown already defined")
      hasTeardown = true
      teardownFn = f
      this
    }

    def withStartBundle(f: DoFn[A, B]#StartBundleContext => Unit): DoFnBuilder[A, B] = {
      require(!hasStartBundle, "StartBundle already defined")
      hasStartBundle = true
      startBundleFn = f
      this
    }

    def withFinishBundle(f: DoFn[A, B]#FinishBundleContext => Unit): DoFnBuilder[A, B] = {
      require(!hasFinishBundle, "FinishBundle already defined")
      hasFinishBundle = true
      finishBundleFn = f
      this
    }

    def build(f: DoFn[A, B]#ProcessContext => Unit): DoFn[A, B] = apply(f)

    def apply(f: DoFn[A, B]#ProcessContext => Unit): DoFn[A, B] =
      new SimpleDoFn(setupFn, teardownFn, startBundleFn, finishBundleFn, f)
  }

  private class SimpleDoFn[A, B](_setupFn: () => Unit,
                                 _teardownFn: () => Unit,
                                 _startBundleFn: DoFn[A, B]#StartBundleContext => Unit,
                                 _finishBundleFn: DoFn[A, B]#FinishBundleContext => Unit,
                                 _f: DoFn[A, B]#ProcessContext => Unit) extends DoFn[A, B] {

    // defeat closure
    val setupFn = ClosureCleaner(_setupFn)
    val teardownFn = ClosureCleaner(_teardownFn)
    val startBundleFn = ClosureCleaner(_startBundleFn)
    val finishBundleFn = ClosureCleaner(_finishBundleFn)
    val f = ClosureCleaner(_f)

    @Setup
    def setup(): Unit = setupFn()

    @Teardown
    def teardown(): Unit = teardownFn()

    @StartBundle
    def startBundle(c: StartBundleContext): Unit = startBundleFn(c)

    @FinishBundle
    def finishBundle(c: FinishBundleContext): Unit = finishBundleFn(c)

    @ProcessElement
    def processElement(c: ProcessContext): Unit = f(c)
  }

}
