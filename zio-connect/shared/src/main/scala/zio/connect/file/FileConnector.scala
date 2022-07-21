package zio.connect.file

import zio.Duration
import zio.stream.ZStream

import java.io.IOException
import java.nio.file.Path

trait FileConnector {
  def listDir(dir: Path): ZStream[Any, IOException, Path]

  def readFile(file: Path): ZStream[Any, IOException, Byte]

  def tailFile(file: Path, freq: Duration): ZStream[Any, IOException, Byte]

  def tailFileUsingWatchService(file: Path, freq: Duration): ZStream[Any, IOException, Byte]
  // def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit]

}

object FileConnector {

  def listDir(dir: Path): ZStream[FileConnector, IOException, Path] =
    ZStream.environmentWithStream(_.get.listDir(dir))

  def readFile(file: Path): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFile(file))

  def tailFile(file: Path, freq: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFile(file, freq))

  def tailFileUsingWatchService(file: Path, freq: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileUsingWatchService(file, freq))

  // def writeFile(file: Path): ZSink[FileConnector, IOException, Chunk[Byte], Unit] =
  // ZIO.accessM[FileConnector](_.get.writeFile(file))

  // def delete: ZSink[FileConnector, IOException, Path, Unit] = ???  // Should it be Stream or Sink?

  // def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???

}
