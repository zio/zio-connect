package zio.connect

import zio._
import zio.stream._
import zio.blocking._
import zio.nio.file.Files

package object file {
  import java.nio.file.Path  // Should we use zio.nio.core.file.Path instead?
  import java.io.IOException

  type FileConnector = Has[FileConnector.Service]
  
  object FileConnector {
    trait Service {
      def listDir(dir: Path): Stream[IOException, Path]

      def readFile(file: Path): Stream[IOException, Chunk[Byte]]
      
      def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit]
      
      // def tailFile(file: Path): ZStream[Blocking, IOException, Chunk[Byte]]
      def tailFile(file: Path): Stream[IOException, Chunk[Byte]]
    }

    object Service {
      val live: Service = new Service {
        def listDir(dir: Path): Stream[IOException, Path] = {
          // Files.list(zio.nio.core.file.Path.fromJava(dir)).provideLayer(Blocking.live).map(_.)
            ZStream.fromEffect(effectBlocking(java.nio.file.Files.list(dir).iterator()))
              .flatMap(Files.fromJavaIterator)
              .provideLayer(Blocking.live)
              .refineOrDie{ case e: IOException => e }
        }

        def readFile(file: Path): Stream[IOException, Chunk[Byte]] = 
          ZStream.fromEffect(Files.readAllBytes(zio.nio.core.file.Path.fromJava(file)).provideLayer(Blocking.live))
          // Is this the way to provide R to API underneath, and hide it?

        def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit] = ???

        def tailFile(file: Path): Stream[IOException, Chunk[Byte]] = {
          val zPath = zio.nio.core.file.Path.fromJava(file)
          ZStream.fromEffect(Files.readAllBytes(zPath).provideLayer(ZEnv.live))
        }
      }

    }

    val any: ZLayer[FileConnector, Nothing, FileConnector] =
      ZLayer.requires[FileConnector]
      // Do we need this?

    val live: Layer[Nothing, FileConnector] =
      ZLayer.succeed(Service.live)
  }

  def listDir(dir: Path): ZStream[FileConnector, IOException, Path] = 
    ZStream.accessStream[FileConnector](_.get.listDir(dir))

  def readFile(file: Path): ZStream[FileConnector, IOException, Chunk[Byte]] = 
    ZStream.accessStream[FileConnector](_.get.readFile(file))

  def tailFile(file: Path): ZStream[FileConnector, IOException, Chunk[Byte]] = 
    ZStream.accessStream[FileConnector](_.get.tailFile(file))

  // def writeFile(file: Path): ZSink[FileConnector, IOException, Chunk[Byte], Unit] = 
    // ZIO.accessM[FileConnector](_.get.writeFile(file))

  def delete: ZSink[FileConnector, IOException, Path, Unit] = ???  // Should it be Stream or Sink?

  def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???
}

