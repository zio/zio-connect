package zio.connect.file.testkit

import zio.{Duration, Scope, Trace, ZLayer}
import zio.connect.file.FileConnector
import zio.stm.{STM, TRef}
import zio.stream.{ZSink, ZStream}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

case class InMemoryFileConnector(fs: Root) extends FileConnector {

  override def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path => fs.delete(path))

  override def deleteRecursivelyPath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] = ???

  override def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrap(fs.list(path).map(a => ZStream.fromChunk(a)))

  override def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(fs.getContent(path).map(a => ZStream.fromChunk(a)))

  override def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] = ???

  override def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] = ???

  override def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      _ <- ZSink.fromZIO(fs.removeContentIfExists(path))
      _ <- ZSink.foreachChunk(bytes => fs.write(path, bytes))
    } yield ()

  override def tempPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempPath)

  override def tempPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempPathIn(dirPath))

  override def tempDirPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] =
    ZSink.fromZIO(fs.tempDirPath)

  override def tempDirPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path] = ???

  override def movePath(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ???

  override def moveFile(locator: File => File)(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    ???

  override def moveFileName(locator: String => String)(implicit
    trace: Trace
  ): ZSink[Any, IOException, String, Nothing, Unit] = ???

  override def moveURI(locator: URI => URI)(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] = ???

  override def existsPath(path: Path)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.fromZIO(fs.exists(path))

}

object InMemoryFileConnector {

  def layer = ZLayer.fromZIO(
    STM.atomically {
      for {
        a <- TRef.make(Map.empty[Path, TKFile])
      } yield InMemoryFileConnector(Root(a))
    }
  )

}
