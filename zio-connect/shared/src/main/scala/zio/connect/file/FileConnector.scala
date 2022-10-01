package zio.connect.file

import zio.stream.{ZSink, ZStream}
import zio.{Duration, Scope, Trace, ZIO}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.Path

trait FileConnector {

  final def deleteFile(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(a.toPath).refineToOrDie[IOException])

  final def deleteFileName(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  final def deleteURI(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  final def deleteRecursivelyFile(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    deleteRecursivelyPath.contramapZIO(a => ZIO.attempt(a.toPath).refineToOrDie[IOException])

  final def deleteRecursivelyFileName(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] =
    deleteRecursivelyPath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  def deleteRecursivelyPath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  final def deleteRecursivelyURI(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    deleteRecursivelyPath.contramapZIO(a => ZIO.attempt(Path.of(a)).refineToOrDie[IOException])

  final def existsFile(file: File)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.unwrap(
      ZIO.attempt(file.toPath).refineToOrDie[IOException].map(path => existsPath(path))
    )

  final def existsFileName(name: String)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.unwrap(
      ZIO.attempt(Path.of(name)).refineToOrDie[IOException].map(path => existsPath(path))
    )

  def existsPath(path: Path)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean]

  final def existsURI(uri: URI)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.unwrap(
      ZIO.attempt(Path.of(uri)).refineToOrDie[IOException].map(path => existsPath(path))
    )

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

  def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  final def listURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, URI] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toUri).refineToOrDie)
    } yield r

  final def moveFile(
    locator: File => File
  )(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] = {
    def fileToPath(f: File => File): Path => Path = { path =>
      f(path.toFile).toPath
    }
    movePath(fileToPath(locator)).contramap[File](file => file.toPath)
  }

  final def moveFileName(
    locator: String => String
  )(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] = {
    def toPath(f: String => String): Path => Path = { path: Path =>
      Path.of(f(path.toString))
    }
    movePath(toPath(locator)).contramap[String](x => Path.of(x))
  }

  def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[Any, IOException, Path, Nothing, Unit]

  //todo extract those locator functions transformers in all moveX somewhere else
  final def moveURI(
    locator: URI => URI
  )(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] = {
    def uriToPath(f: URI => URI): Path => Path = { path =>
      Path.of(f(path.toUri))
    }
    movePath(uriToPath(locator)).contramap[URI](uri => Path.of(uri))
  }

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

  def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  final def readURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

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

  def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  final def tailURI(uri: => URI, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

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

  def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte]

  final def tailURIUsingWatchService(uri: => URI, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  final def tempFile(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))

  final def tempFileName(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))

  def tempPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempURI(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))

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

  def tempPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempURIIn(dirURI: URI)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirURI)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))
      )

  final def tempDirFile(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))

  final def tempDirFileName(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))

  def tempDirPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempDirURI(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    tempDirPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))

  final def tempDirFileIn(dirFile: File)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    ZSink
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempDirPathIn(dirPath)
          .flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))
      )

  final def tempDirFileNameIn(dirName: String)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirName)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempDirPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))
      )

  def tempDirPathIn(dirPath: Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempDirURIIn(dirURI: URI)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirURI)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempDirPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))
      )

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

  def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

  final def writeURI(uri: => URI)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(Path.of(uri)).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

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
