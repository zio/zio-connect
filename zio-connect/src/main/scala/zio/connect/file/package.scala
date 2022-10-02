package zio.connect

import zio.stream.{ZSink, ZStream}
import zio.{Duration, Scope, Trace, ZIO}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

package object file {

  val live = LiveFileConnector.layer
  val test = TestFileConnector.layer

  def listPath(path: => Path): ZStream[FileConnector, IOException, Path] =
    ZStream.environmentWithStream(_.get.listPath(path))

  def listFile(file: => File): ZStream[FileConnector, IOException, File] =
    ZStream.environmentWithStream(_.get.listFile(file))

  def listFileName(name: => String): ZStream[FileConnector, IOException, String] =
    ZStream.environmentWithStream(_.get.listFileName(name))

  def listURI(uri: => URI): ZStream[FileConnector, IOException, URI] =
    ZStream.environmentWithStream(_.get.listURI(uri))

  def readPath(path: => Path): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readPath(path))

  def readFile(file: => File): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFile(file))

  def readFileName(name: => String): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFileName(name))

  def readURI(uri: => URI): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readURI(uri))

  def writePath(path: => Path): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writePath(path))

  def writeFile(file: => File): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFile(file))

  def writeFileName(name: => String): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFileName(name))

  def writeURI(uri: => URI): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeURI(uri))

  def tempPath(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempPath)

  def tempFile(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFile)

  def tempFileName(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileName)

  def tempURI(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempURI)

  def tempPathIn(path: Path)(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempPathIn(path))

  def tempFileIn(file: File)(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileIn(file))

  def tempFileNameIn(name: String)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileNameIn(name))

  def tempURIIn(uri: URI)(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempURIIn(uri))

  def tempDirPath(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirPath)

  def tempDirFile(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFile)

  def tempDirFileName(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileName)

  def tempDirURI(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirURI)

  def tempDirPathIn(path: Path)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirPathIn(path))

  def tempDirFileIn(file: File)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileIn(file))

  def tempDirFileNameIn(name: String)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileNameIn(name))

  def tempDirURIIn(uri: URI)(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirURIIn(uri))

  def deletePath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deletePath)

  def deleteFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFile)

  def deleteFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFileName)

  def deleteURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteURI)

  def deleteRecursivelyPath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyPath)

  def deleteRecursivelyFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyFile)

  def deleteRecursivelyFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyFileName)

  def deleteRecursivelyURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyURI)

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.movePath(locator))

  def movePathZIO(locator: Path => ZIO[Any, IOException, Path])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.movePathZIO(locator))

  def moveFile(locator: File => File)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFile(locator))

  def moveFileZIO(locator: File => ZIO[Any, IOException, File])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFileZIO(locator))

  def moveFileName(locator: String => String)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFileName(locator))

  def moveFileNameZIO(locator: String => ZIO[Any, IOException, String])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFileNameZIO(locator))

  def moveURI(locator: URI => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveURI(locator))

  def moveURIZIO(locator: URI => ZIO[Any, IOException, URI])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveURIZIO(locator))

  def tailPath(path: => Path, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailPath(path, duration))

  def tailFile(file: => File, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFile(file, duration))

  def tailFileName(name: => String, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileName(name, duration))

  def tailURI(uri: => URI, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailURI(uri, duration))

  def tailPathUsingWatchService(path: => Path, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailPathUsingWatchService(path, duration))

  def tailFileUsingWatchService(file: => File, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileUsingWatchService(file, duration))

  def tailFileNameUsingWatchService(name: => String, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileNameUsingWatchService(name, duration))

  def tailURIUsingWatchService(uri: => URI, duration: Duration): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailURIUsingWatchService(uri, duration))

  def existsPath(path: Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsPath(path))

  def existsFile(file: File)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsFile(file))

  def existsFileName(name: String)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsFileName(name))

  def existsURI(uri: URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsURI(uri))

}
