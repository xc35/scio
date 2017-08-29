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

package com.spotify.scio.tensorflow.example

/**
 * Feature name feature specification. In most cases you want to use
 * [[https://github.com/spotify/featran featran]], which would give you feature specification for
 * free.
 *
 * {{{
 * object WordCountFeatureSpec {
 *   val featureNameSpec = FeatureNameFeatureSpec[WordCountFeatures]
 *   case class WordCountFeatures(wordLength: Float, count: Float)
 * }
 * }}}
 *
 * @note uses reflection to fetch field names, should not be used in performance critical path.
 */
object FeatureNameFeatureSpec {
  import scala.reflect.runtime.universe._
  def apply[T: TypeTag]: Seq[String] = {
    require(typeOf[T].typeSymbol.isClass && typeOf[T].typeSymbol.asClass.isCaseClass,
      "Type must be a case class")
    typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.decodedName.toString
    }.toSeq
  }
}
