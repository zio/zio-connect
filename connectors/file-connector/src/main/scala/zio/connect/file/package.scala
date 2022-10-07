package zio.connect

import zio.stream.{ZSink, ZStream}
import zio.{Duration, Scope, Trace, ZIO}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

package object file {

  val live = LiveFileConnector.layer
  val test = TestFileConnector.layer

  def deleteFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFile)

  def deleteFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFileName)

  def deletePath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deletePath)

  def deleteURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteURI)

  def deleteRecursivelyFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyFile)

  def deleteRecursivelyFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyFileName)

  def deleteRecursivelyPath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyPath)

  def deleteRecursivelyURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteRecursivelyURI)

  def existsFile(file: => File)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsFile(file))

  def existsFileName(name: => String)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsFileName(name))

  def existsPath(path: => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsPath(path))

  def existsURI(uri: => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Any, Nothing, Boolean] =
    ZSink.environmentWithSink[FileConnector](_.get.existsURI(uri))

  def listFile(file: => File)(implicit trace: Trace): ZStream[FileConnector, IOException, File] =
    ZStream.environmentWithStream(_.get.listFile(file))

  def listFileName(name: => String)(implicit trace: Trace): ZStream[FileConnector, IOException, String] =
    ZStream.environmentWithStream(_.get.listFileName(name))

  def listPath(path: => Path)(implicit trace: Trace): ZStream[FileConnector, IOException, Path] =
    ZStream.environmentWithStream(_.get.listPath(path))

  def listURI(uri: => URI)(implicit trace: Trace): ZStream[FileConnector, IOException, URI] =
    ZStream.environmentWithStream(_.get.listURI(uri))

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

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.movePath(locator))

  def movePathZIO(locator: Path => ZIO[Any, IOException, Path])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.movePathZIO(locator))

  def moveURI(locator: URI => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveURI(locator))

  def moveURIZIO(locator: URI => ZIO[Any, IOException, URI])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveURIZIO(locator))

  def readFile(file: => File)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFile(file))

  def readFileName(name: => String)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readFileName(name))

  def readPath(path: => Path)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readPath(path))

  def readURI(uri: => URI)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.readURI(uri))
  def tempFile(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFile)

  def tailFile(file: => File, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFile(file, duration))

  def tailFileUsingWatchService(file: => File, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileUsingWatchService(file, duration))

  def tailFileName(name: => String, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileName(name, duration))

  def tailFileNameUsingWatchService(name: => String, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailFileNameUsingWatchService(name, duration))

  def tailPath(path: => Path, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailPath(path, duration))

  def tailPathUsingWatchService(path: => Path, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailPathUsingWatchService(path, duration))

  def tailURI(uri: => URI, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailURI(uri, duration))

  def tailURIUsingWatchService(uri: => URI, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.environmentWithStream(_.get.tailURIUsingWatchService(uri, duration))

  def tempFileIn(file: => File)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileIn(file))

  def tempFileName(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileName)

  def tempFileNameIn(name: => String)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempFileNameIn(name))

  def tempPath(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempPath)

  def tempPathIn(path: => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempPathIn(path))

  def tempURI(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempURI)

  def tempURIIn(uri: => URI)(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempURIIn(uri))

  def tempDirFile(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFile)

  def tempDirFileIn(file: => File)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, File] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileIn(file))

  def tempDirFileName(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileName)

  def tempDirFileNameIn(name: => String)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, String] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirFileNameIn(name))

  def tempDirPath(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirPath)

  def tempDirPathIn(path: => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, Path] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirPathIn(path))

  def tempDirURI(implicit trace: Trace): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirURI)

  def tempDirURIIn(uri: => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector with Scope, IOException, Byte, Nothing, URI] =
    ZSink.environmentWithSink[FileConnector](_.get.tempDirURIIn(uri))

  def writeFile(file: => File)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFile(file))

  def writeFileName(name: => String)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeFileName(name))

  def writePath(path: => Path)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writePath(path))

  def writeURI(uri: => URI)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.writeURI(uri))
}
