package zio.connect.file

import zio.{Duration, Scope, Trace}
import zio.stream.{ZSink, ZStream}
import java.nio.file.Path

import java.io.IOException

trait FileConnector {
  def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte]

  def writeFile(file: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

  def deleteFile(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  def moveFile(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

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

  /**
   * Creates a file at given path and writes to it. If the path is to an already
   * existing file then the file contents will be overwritten.
   */
  def writeFile(file: => Path): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFile(file))

  val deleteFile: ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFile)

  def moveFile(locator: java.nio.file.Path => java.nio.file.Path): ZSink[FileConnector, IOException, java.nio.file.Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFile(locator))

}
