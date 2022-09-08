package zio.connect.file

import java.nio.file.Path
import zio.{Duration, Scope, Trace, ZIO}
import zio.stream.{ZSink, ZStream}

import java.io.{File, IOException}
import java.net.URI

trait FileConnector {

  def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  final def listFile(file: => File)(implicit trace: Trace): ZStream[Any, IOException, File] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toFile).refineToOrDie)
    } yield r

  final def listFileName(name: => String)(implicit trace: Trace): ZStream[Any, IOException, String] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(name)).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toString).refineToOrDie)
    } yield r

  final def listURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, URI] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toUri).refineToOrDie)
    } yield r

  def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  final def readFile(file: => File)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  final def readFileName(name: => String)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(name)).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  final def readURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  final def tailFile(file: => File, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  final def tailFileName(name: => String, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(name)).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  final def tailURI(uri: => URI, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte]

  final def tailFileUsingWatchService(file: => File, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  final def tailFileNameUsingWatchService(fileName: => String, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(fileName)).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  final def tailURIUsingWatchService(uri: => URI, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

  final def writeFile(file: => File)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

  final def writeFileName(name: => String)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(Path.of(name)).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

  final def writeURI(uri: => URI)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

  def tempPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempFile(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))

  final def tempFileName(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))

  final def tempURI(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))

  def tempPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempFileIn(dirFile: File)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    ZSink
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath)
          .flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))
      )

  final def tempFileNameIn(dirName: String)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirName)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))
      )

  final def tempURIIn(dirURI: URI)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirURI)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))
      )

  def tempDirPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempDirFile(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))

  final def tempDirFileName(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))

  final def tempDirURI(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))

  def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  final def deleteFile(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(a.toPath).refineToOrDie[IOException])

  final def deleteFileName(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  final def deleteURI(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[Any, IOException, Path, Nothing, Unit]

  def moveFile(
    locator: File => File
  )(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit]

  def moveFileName(
    locator: String => String
  )(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit]

  def moveURI(
    locator: URI => URI
  )(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit]
}

object FileConnector {

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

  def deletePath(implicit trace: Trace): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deletePath)

  def deleteFile(implicit trace: Trace): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFile)

  def deleteFileName(implicit trace: Trace): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteFileName)

  def deleteURI(implicit trace: Trace): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.deleteURI)

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, Path, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.movePath(locator))

  def moveFile(locator: File => File)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, File, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFile(locator))

  def moveFileName(locator: String => String)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, String, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveFileName(locator))

  def moveURI(locator: URI => URI)(implicit
    trace: Trace
  ): ZSink[FileConnector, IOException, URI, Nothing, Unit] =
    ZSink.environmentWithSink(_.get.moveURI(locator))

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

}
