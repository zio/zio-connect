package zio.connect.file

import zio.nio.file.Path
import zio.{Duration, Scope, Trace}
import zio.stream.{ZSink, ZStream}

import java.io.IOException

trait FileConnector {
  def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Scope, IOException, Byte]

  def writeFile(file: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

}

object FileConnector {

  def listDir(dir: => Path): ZStream[FileConnector, IOException, Path] =
    ZStream.environmentWithStream(_.get.listDir(dir))

  def readFile(file: => Path): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFile(file))

  def tailFile(file: => Path, freq: => Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFile(file, freq))

  def tailFileUsingWatchService(
    file: => Path,
    freq: => Duration
  ): ZStream[FileConnector with Scope, IOException, Byte] =
    ZStream.environmentWithStream[FileConnector](_.get.tailFileUsingWatchService(file, freq))

  def writeFile(file: => Path): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFile(file))

  // def delete: ZSink[FileConnector, IOException, Path, Unit] = ???  // Should it be Stream or Sink?

  // def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???

}
