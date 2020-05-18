package zio.connect

import zio._
import zio.stream._

package object file {
  // Powered by ZIO NIO
  import java.nio.file.Path 
  import java.io.IOException

  type FileConnector = Has[FileConnector.Service]
  
  object FileConnector {
    trait Service {
      def listPaths(path: Path): Stream[IOException, Path]
    }
  }

  def listPaths(path: Path): ZStream[FileConnector, IOException, Path] = 
    ZStream.accessStream[FileConnector](_.get.listPaths(path))

  def readPath(path: Path): ZStream[FileConnector, IOException, Byte] = ???

  def writePath(path: Path): ZSink[FileConnector, IOException, Byte, Unit] = ???

  def delete: ZSink[FileConnector, IOException, Path, Unit] = ???

  def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???
}

