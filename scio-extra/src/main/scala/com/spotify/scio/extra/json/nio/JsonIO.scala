/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.extra.json.nio

import com.spotify.scio.ScioContext
import com.spotify.scio.extra.json.DecodeError
import com.spotify.scio.io.Tap
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.values.SCollection
import io.circe.{Decoder, Encoder, Printer}
import io.circe.generic.AutoDerivation
import io.circe.parser._
import io.circe.syntax._
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.{io => gio}

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.{Left, Right}

trait JsonIO[T] extends ScioIO[T]

case class JsonRead[T: ClassTag : Decoder](path: String)
  extends JsonIO[Either[DecodeError, T]] {

  case class WriteParams(printer: Printer = Printer.noSpaces,
                         numShards: Int = 0,
                         compression: Compression = Compression.UNCOMPRESSED)

  override type ReadP = Unit
  override type WriteP = Unit
  override def read(sc: ScioContext, params: ReadP): SCollection[Either[DecodeError, T]] =
    if (sc.isTest) {
      sc.getTestInput[Either[DecodeError, T]](this)
    } else {
      sc
        .wrap(sc.applyInternal(gio.TextIO.read().from(path))).setName(path)
        .map { json =>
          decode[T](json) match {
            case Left(e) => Left(DecodeError(e, json))
            case Right(t) => Right(t)
          }
        }
    }

  override def write(data: SCollection[Either[DecodeError, T]], params: WriteP)
  : Future[Tap[Either[DecodeError, T]]] =
    throw new IllegalStateException("JsonRead is read-only")

  override def tap(read: ReadP): Tap[Either[DecodeError, T]] = ???

  override def id: String = path
}

case class JsonWrite[T: ClassTag : Encoder](path: String) extends JsonIO[T] {
  case class WriteParams(printer: Printer = Printer.noSpaces,
                         numShards: Int = 0,
                         compression: Compression = Compression.UNCOMPRESSED)

  override type ReadP = Nothing
  override type WriteP = WriteParams

  override def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new IllegalStateException("JsonWrite is write-only")

  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = ???

  override def tap(read: ReadP): Tap[T] =
    throw new IllegalStateException("JsonWrite is write-only")

  override def id: String = path
}
