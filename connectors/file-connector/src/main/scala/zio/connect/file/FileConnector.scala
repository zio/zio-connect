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

  /**
   *  Will fail for non empty directories. For that use case use deleteRecursivelyPath
   */
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

  final def existsFile(file: => File)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.unwrap(
      ZIO.attempt(file.toPath).refineToOrDie[IOException].map(path => existsPath(path))
    )

  final def existsFileName(name: => String)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
    ZSink.unwrap(
      ZIO.attempt(Path.of(name)).refineToOrDie[IOException].map(path => existsPath(path))
    )

  def existsPath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean]

  final def existsURI(uri: => URI)(implicit trace: Trace): ZSink[Any, IOException, Any, Nothing, Boolean] =
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

  final def moveFileZIO(locator: File => ZIO[Any, IOException, File])(implicit
    trace: Trace
  ): ZSink[Any, IOException, File, Nothing, Unit] = {
    def toPath(f: File => ZIO[Any, IOException, File]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toFile).map(_.toPath)
    }

    movePathZIO(toPath(locator)).contramap[File](_.toPath)
  }

  final def moveFileName(
    locator: String => String
  )(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] = {
    def toPath(f: String => String): Path => Path = { (path: Path) =>
      Path.of(f(path.toString))
    }
    movePath(toPath(locator)).contramap[String](x => Path.of(x))
  }

  final def moveFileNameZIO(locator: String => ZIO[Any, IOException, String])(implicit
    trace: Trace
  ): ZSink[Any, IOException, String, Nothing, Unit] = {
    def toPath(f: String => ZIO[Any, IOException, String]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toString).map(a => Path.of(a))
    }

    movePathZIO(toPath(locator)).contramap[String](a => Path.of(a))
  }

  final def movePath(locator: Path => Path)(implicit
    trace: Trace
  ): ZSink[Any, IOException, Path, Nothing, Unit] = {
    def toZIOLocator(f: Path => Path): Path => ZIO[Any, IOException, Path] = { path =>
      ZIO.attempt(f(path)).refineToOrDie[IOException]
    }

    movePathZIO(toZIOLocator(locator))
  }

  def movePathZIO(locator: Path => ZIO[Any, IOException, Path])(implicit
    trace: Trace
  ): ZSink[Any, IOException, Path, Nothing, Unit]

  final def moveURI(
    locator: URI => URI
  )(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] = {
    def uriToPath(f: URI => URI): Path => Path = { path =>
      Path.of(f(path.toUri))
    }
    movePath(uriToPath(locator)).contramap[URI](uri => Path.of(uri))
  }

  final def moveURIZIO(locator: URI => ZIO[Any, IOException, URI])(implicit
    trace: Trace
  ): ZSink[Any, IOException, URI, Nothing, Unit] = {
    def uriToPath(f: URI => ZIO[Any, IOException, URI]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toUri).map(Path.of)
    }

    movePathZIO(uriToPath(locator)).contramap[URI](Path.of)
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

  final def tempFileIn(dirFile: => File)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    ZSink
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath)
          .flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))
      )

  final def tempFileName(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))

  final def tempFileNameIn(dirName: => String)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirName)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))
      )

  def tempPath(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  def tempPathIn(dirPath: => Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempURI(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
    tempPath.flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toUri).refineToOrDie[IOException]))

  final def tempURIIn(dirURI: => URI)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
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

  final def tempDirFileIn(dirFile: => File)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, File] =
    ZSink
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempDirPathIn(dirPath)
          .flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toFile).refineToOrDie[IOException]))
      )

  final def tempDirFileNameIn(
    dirName: => String
  )(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, String] =
    ZSink
      .fromZIO(ZIO.attempt(Path.of(dirName)).refineToOrDie[IOException])
      .flatMap(dirPath =>
        tempDirPathIn(dirPath).flatMap(p => ZSink.fromZIO(ZIO.attempt(p.toString).refineToOrDie[IOException]))
      )

  def tempDirPathIn(dirPath: => Path)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, Path]

  final def tempDirURIIn(dirURI: => URI)(implicit trace: Trace): ZSink[Scope, IOException, Any, Nothing, URI] =
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
