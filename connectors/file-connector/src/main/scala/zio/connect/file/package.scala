package zio.connect

import zio.stream.{ZSink, ZStream}
import zio.{Duration, Trace, ZIO}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

package object file {

  def deleteFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteFile)

  def deleteFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteFileName)

  def deleteFileNameRecursively(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteFileNameRecursively)

  def deleteFileRecursively(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteFileRecursively)

  def deletePath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.serviceWithSink(_.deletePath)

  def deletePathRecursively(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.serviceWithSink(_.deletePathRecursively)

  def deleteURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteURI)

  def deleteURIRecursively(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.serviceWithSink(_.deleteURIRecursively)

  def existsFile(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, File, Boolean] =
    ZSink.serviceWithSink(_.existsFile)

  def existsFileName(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, String, Boolean] =
    ZSink.serviceWithSink(_.existsFileName)

  def existsPath(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Path, Boolean] =
    ZSink.serviceWithSink(_.existsPath)

  def existsURI(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, URI, Boolean] =
    ZSink.serviceWithSink(_.existsURI)

  val fileConnectorLiveLayer = LiveFileConnector.layer
  val fileConnectorTestLayer = TestFileConnector.layer

  def listFile(file: => File)(implicit trace: Trace): ZStream[FileConnector, IOException, File] =
    ZStream.serviceWithStream(_.listFile(file))

  def listFileName(name: => String)(implicit trace: Trace): ZStream[FileConnector, IOException, String] =
    ZStream.serviceWithStream(_.listFileName(name))

  def listPath(path: => Path)(implicit trace: Trace): ZStream[FileConnector, IOException, Path] =
    ZStream.serviceWithStream(_.listPath(path))

  def listURI(uri: => URI)(implicit trace: Trace): ZStream[FileConnector, IOException, URI] =
    ZStream.serviceWithStream(_.listURI(uri))

  def moveFile(locator: File => File)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveFile(locator))

  def moveFileName(locator: String => String)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveFileName(locator))

  def moveFileNameZIO(locator: String => ZIO[Any, IOException, String])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveFileNameZIO(locator))

  def moveFileZIO(locator: File => ZIO[Any, IOException, File])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveFileZIO(locator))

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.serviceWithSink(_.movePath(locator))

  def movePathZIO(locator: Path => ZIO[Any, IOException, Path])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.serviceWithSink(_.movePathZIO(locator))

  def moveURI(locator: URI => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveURI(locator))

  def moveURIZIO(locator: URI => ZIO[Any, IOException, URI])(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.serviceWithSink(_.moveURIZIO(locator))

  def readFile(file: => File)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.readFile(file))

  def readFileName(name: => String)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.readFileName(name))

  def readPath(path: => Path)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.readPath(path))

  def readURI(uri: => URI)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.readURI(uri))

  def tailFile(file: => File, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailFile(file, duration))

  def tailFileName(name: => String, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailFileName(name, duration))

  def tailFileNameUsingWatchService(name: => String, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailFileNameUsingWatchService(name, duration))

  def tailFileUsingWatchService(file: => File, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailFileUsingWatchService(file, duration))

  def tailPath(path: => Path, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailPath(path, duration))

  def tailPathUsingWatchService(path: => Path, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailPathUsingWatchService(path, duration))

  def tailURI(uri: => URI, duration: => Duration)(implicit trace: Trace): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailURI(uri, duration))

  def tailURIUsingWatchService(uri: => URI, duration: => Duration)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Byte] =
    ZStream.serviceWithStream(_.tailURIUsingWatchService(uri, duration))

  def tempDirFile(implicit trace: Trace): ZStream[FileConnector, IOException, File] =
    ZStream.serviceWithStream(_.tempDirFile)

  def tempDirFileIn(file: => File)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, File] =
    ZStream.serviceWithStream(_.tempDirFileIn(file))

  def tempDirFileName(implicit trace: Trace): ZStream[FileConnector, IOException, String] =
    ZStream.serviceWithStream(_.tempDirFileName)

  def tempDirFileNameIn(name: => String)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, String] =
    ZStream.serviceWithStream(_.tempDirFileNameIn(name))

  def tempDirPath(implicit trace: Trace): ZStream[FileConnector, IOException, Path] =
    ZStream.serviceWithStream[FileConnector](_.tempDirPath)

  def tempDirPathIn(path: => Path)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Path] =
    ZStream.serviceWithStream[FileConnector](_.tempDirPathIn(path))

  def tempDirURI(implicit trace: Trace): ZStream[FileConnector, IOException, URI] =
    ZStream.serviceWithStream[FileConnector](_.tempDirURI)

  def tempDirURIIn(uri: => URI)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, URI] =
    ZStream.serviceWithStream[FileConnector](_.tempDirURIIn(uri))

  def tempFile(implicit trace: Trace): ZStream[FileConnector, IOException, File] =
    ZStream.serviceWithStream[FileConnector](_.tempFile)

  def tempFileIn(file: => File)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, File] =
    ZStream.serviceWithStream[FileConnector](_.tempFileIn(file))

  def tempFileName(implicit trace: Trace): ZStream[FileConnector, IOException, String] =
    ZStream.serviceWithStream[FileConnector](_.tempFileName)

  def tempFileNameIn(name: => String)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, String] =
    ZStream.serviceWithStream[FileConnector](_.tempFileNameIn(name))

  def tempPath(implicit trace: Trace): ZStream[FileConnector, IOException, Path] =
    ZStream.serviceWithStream[FileConnector](_.tempPath)

  def tempPathIn(path: => Path)(implicit
    trace: Trace
  ): ZStream[FileConnector, IOException, Path] =
    ZStream.serviceWithStream[FileConnector](_.tempPathIn(path))

  def tempURI(implicit trace: Trace): ZStream[FileConnector, IOException, URI] =
    ZStream.serviceWithStream[FileConnector](_.tempURI)

  def tempURIIn(uri: => URI)(implicit trace: Trace): ZStream[FileConnector, IOException, URI] =
    ZStream.serviceWithStream[FileConnector](_.tempURIIn(uri))

  def writeFile(file: => File)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.writeFile(file))

  def writeFileName(name: => String)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.writeFileName(name))

  def writePath(path: => Path)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.writePath(path))

  def writeURI(uri: => URI)(implicit trace: Trace): ZSink[FileConnector, IOException, Byte, Nothing, Unit] =
    ZSink.serviceWithSink(_.writeURI(uri))
}
