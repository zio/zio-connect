package zio.connect.file.testkit

import zio.connect.file.FileConnector
import zio.stm.{STM, TRef}
import zio.stream.{ZSink, ZStream}
import zio.{Duration, Queue, Ref, Schedule, Scope, Trace, ZIO, ZLayer}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

case class InMemoryFileConnector(fs: Root) extends FileConnector {

  override def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path => fs.delete(path))

  override def deleteRecursivelyPath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path => fs.deleteRecursively(path))

  override def existsPath(path: Path)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.fromZIO(fs.exists(path))

  override def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrap(fs.list(path).map(a => ZStream.fromChunk(a)))

  override def moveFile(locator: File => File)(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    ZSink.foreach { file =>
      fs.movePath(file.toPath, locator(file).toPath)
    }

  override def moveFileName(locator: String => String)(implicit
    trace: Trace
  ): ZSink[Any, IOException, String, Nothing, Unit] =
    ZSink.foreach { name =>
      fs.movePath(Path.of(name), Path.of(locator(name)))
    }

  override def movePath(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach { oPath =>
      fs.movePath(oPath, locator(oPath))
    }

  override def moveURI(locator: URI => URI)(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    ZSink.foreach { uri =>
      fs.movePath(Path.of(uri), Path.of(locator(uri)))
    }

  override def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(fs.getContent(path).map(a => ZStream.fromChunk(a)))

  override def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] = {

    def push(index: Ref[Int], queue: Queue[Byte]): ZIO[Any, IOException, Unit] =
      for {
        content   <- fs.getContent(path)
        idx       <- index.get
        newContent = content.drop(idx)
        _         <- queue.offerAll(newContent)
        newIndex   = content.length
        _         <- index.set(newIndex)

      } yield ()

    ZStream.unwrap(
      for {
        queue <- Queue.unbounded[Byte]
        index <- Ref.make(0)
        _     <- push(index, queue).repeat(Schedule.recurs(10) && Schedule.spaced(Duration.fromMillis(100)))
      } yield ZStream.fromQueue(queue)
    )

  }

  override def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] = tailPath(path, freq)

  override def tempPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempPath)

  override def tempPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempPathIn(dirPath))

  override def tempDirPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempDirPath)

  override def tempDirPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempDirPathIn(dirPath))

  override def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      _ <- ZSink.fromZIO(fs.removeContentIfExists(path))
      _ <- ZSink.foreachChunk(bytes => fs.write(path, bytes))
    } yield ()

}

object InMemoryFileConnector {

  def layer: ZLayer[Any, Nothing, InMemoryFileConnector] = ZLayer.fromZIO(
    STM.atomically {
      for {
        a <- TRef.make(Map.empty[Path, TKFile])
      } yield InMemoryFileConnector(Root(a))
    }
  )

}
