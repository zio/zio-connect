package zio.connect.file

import zio.stream.{ZSink, ZStream}
import zio.{Duration, Trace, ZIO}

import java.io.{File, IOException}
import java.net.URI
import java.nio.file.{Path, Paths}

trait FileConnector {

  final def deleteFile(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(a.toPath).refineToOrDie[IOException])

  final def deleteFileName(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Paths.get(a)).refineToOrDie[IOException])

  final def deleteFileNameRecursively(implicit trace: Trace): ZSink[Any, IOException, String, Nothing, Unit] =
    deletePathRecursively.contramapZIO(a => ZIO.attempt(Paths.get(a)).refineToOrDie[IOException])

  final def deleteFileRecursively(implicit trace: Trace): ZSink[Any, IOException, File, Nothing, Unit] =
    deletePathRecursively.contramapZIO(a => ZIO.attempt(a.toPath).refineToOrDie[IOException])

  /**
   *  Will fail for non empty directories. For that use case use deletePathRecursively
   */
  def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  def deletePathRecursively(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit]

  final def deleteURI(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    deletePath.contramapZIO(a => ZIO.attempt(Paths.get(a)).refineToOrDie[IOException])

  final def deleteURIRecursively(implicit trace: Trace): ZSink[Any, IOException, URI, Nothing, Unit] =
    deletePathRecursively.contramapZIO(a => ZIO.attempt(Paths.get(a)).refineToOrDie[IOException])

  final def existsFile(implicit trace: Trace): ZSink[Any, IOException, File, File, Boolean] =
    existsPath
      .contramapZIO[Any, IOException, File](file => ZIO.attempt(file.toPath).refineToOrDie[IOException])
      .toChannel
      .mapOutZIO(paths => ZIO.foreach(paths)(a => ZIO.attempt(a.toFile).refineToOrDie[IOException]))
      .toSink

  final def existsFileName(implicit trace: Trace): ZSink[Any, IOException, String, String, Boolean] =
    existsPath
      .contramapZIO[Any, IOException, String](name => ZIO.attempt(Paths.get(name)).refineToOrDie[IOException])
      .toChannel
      .mapOutZIO(paths => ZIO.foreach(paths)(a => ZIO.attempt(a.toString).refineToOrDie[IOException]))
      .toSink

  def existsPath(implicit trace: Trace): ZSink[Any, IOException, Path, Path, Boolean]

  final def existsURI(implicit trace: Trace): ZSink[Any, IOException, URI, URI, Boolean] =
    existsPath
      .contramapZIO[Any, IOException, URI](uri => ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
      .toChannel
      .mapOutZIO(paths => ZIO.foreach(paths)(a => ZIO.attempt(a.toUri).refineToOrDie[IOException]))
      .toSink

  final def listFile(file: => File)(implicit trace: Trace): ZStream[Any, IOException, File] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toFile).refineToOrDie)
    } yield r

  final def listFileName(name: => String)(implicit trace: Trace): ZStream[Any, IOException, String] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(name)).refineToOrDie[IOException])
      r    <- listPath(path).mapZIO(a => ZIO.attempt(a.toString).refineToOrDie)
    } yield r

  def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  final def listURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, URI] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
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
    def toPath(f: String => String): Path => Path = { (path: Path) =>
      Paths.get(f(path.toString))
    }

    movePath(toPath(locator)).contramap[String](x => Paths.get(x))
  }

  final def moveFileZIO(locator: File => ZIO[Any, IOException, File])(implicit
    trace: Trace
  ): ZSink[Any, IOException, File, Nothing, Unit] = {
    def toPath(f: File => ZIO[Any, IOException, File]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toFile).map(_.toPath)
    }

    movePathZIO(toPath(locator)).contramap[File](_.toPath)
  }

  final def moveFileNameZIO(locator: String => ZIO[Any, IOException, String])(implicit
    trace: Trace
  ): ZSink[Any, IOException, String, Nothing, Unit] = {
    def toPath(f: String => ZIO[Any, IOException, String]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toString).map(a => Paths.get(a))
    }

    movePathZIO(toPath(locator)).contramap[String](a => Paths.get(a))
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
      Paths.get(f(path.toUri))
    }
    movePath(uriToPath(locator)).contramap[URI](uri => Paths.get(uri))
  }

  final def moveURIZIO(locator: URI => ZIO[Any, IOException, URI])(implicit
    trace: Trace
  ): ZSink[Any, IOException, URI, Nothing, Unit] = {
    def uriToPath(f: URI => ZIO[Any, IOException, URI]): Path => ZIO[Any, IOException, Path] = { path =>
      f(path.toUri).map(Paths.get)
    }

    movePathZIO(uriToPath(locator)).contramap[URI](Paths.get)
  }

  final def readFile(file: => File)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  final def readFileName(name: => String)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(name)).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  final def readURI(uri: => URI)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
      r    <- readPath(path)
    } yield r

  final def tailFile(file: => File, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  final def tailFileName(name: => String, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(name)).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  final def tailFileNameUsingWatchService(fileName: => String, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(fileName)).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  final def tailFileUsingWatchService(file: => File, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte]

  def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte]

  final def tailURI(uri: => URI, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
      r    <- tailPath(path, freq)
    } yield r

  final def tailURIUsingWatchService(uri: => URI, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    for {
      path <- ZStream.fromZIO(ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
      r    <- tailPathUsingWatchService(path, freq)
    } yield r

  final def tempDirFile(implicit trace: Trace): ZStream[Any, IOException, File] =
    tempDirPath.mapZIO(p => ZIO.attempt(p.toFile).refineToOrDie[IOException])

  final def tempDirFileIn(dirFile: => File)(implicit trace: Trace): ZStream[Any, IOException, File] =
    ZStream
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(p => tempDirPathIn(p))
      .mapZIO(p => ZIO.attempt(p.toFile).refineToOrDie[IOException])

  final def tempDirFileName(implicit trace: Trace): ZStream[Any, IOException, String] =
    tempDirPath.mapZIO(p => ZIO.attempt(p.toString).refineToOrDie[IOException])

  final def tempDirFileNameIn(
    dirName: => String
  )(implicit trace: Trace): ZStream[Any, IOException, String] =
    ZStream
      .fromZIO(ZIO.attempt(Paths.get(dirName)).refineToOrDie[IOException])
      .flatMap(p => tempDirPathIn(p))
      .mapZIO(p => ZIO.attempt(p.toString).refineToOrDie[IOException])

  def tempDirPath(implicit trace: Trace): ZStream[Any, IOException, Path]

  def tempDirPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  final def tempDirURI(implicit trace: Trace): ZStream[Any, IOException, URI] =
    tempDirPath.mapZIO(p => ZIO.attempt(p.toUri).refineToOrDie[IOException])

  final def tempDirURIIn(dirURI: => URI)(implicit trace: Trace): ZStream[Any, IOException, URI] =
    ZStream
      .fromZIO(ZIO.attempt(Paths.get(dirURI)).refineToOrDie[IOException])
      .flatMap(p => tempDirPathIn(p))
      .mapZIO(p => ZIO.attempt(p.toUri).refineToOrDie[IOException])

  final def tempFile(implicit trace: Trace): ZStream[Any, IOException, File] =
    tempPath.mapZIO(p => ZIO.attempt(p.toFile).refineToOrDie[IOException])

  final def tempFileIn(dirFile: => File)(implicit trace: Trace): ZStream[Any, IOException, File] =
    ZStream
      .fromZIO(ZIO.attempt(dirFile.toPath).refineToOrDie[IOException])
      .flatMap(dirPath => tempPathIn(dirPath))
      .flatMap(path => ZStream.fromZIO(ZIO.attempt(path.toFile).refineToOrDie[IOException]))

  final def tempFileName(implicit trace: Trace): ZStream[Any, IOException, String] =
    tempPath.mapZIO(p => ZIO.attempt(p.toString).refineToOrDie[IOException])

  final def tempFileNameIn(dirName: => String)(implicit trace: Trace): ZStream[Any, IOException, String] =
    ZStream
      .fromZIO(ZIO.attempt(Paths.get(dirName)).refineToOrDie[IOException])
      .flatMap(dirPath => tempPathIn(dirPath))
      .flatMap(path => ZStream.fromZIO(ZIO.attempt(path.toString).refineToOrDie[IOException]))

  def tempPath(implicit trace: Trace): ZStream[Any, IOException, Path]

  def tempPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  final def tempURI(implicit trace: Trace): ZStream[Any, IOException, URI] =
    tempPath.mapZIO(p => ZIO.attempt(p.toUri).refineToOrDie[IOException])

  final def tempURIIn(dirURI: => URI)(implicit trace: Trace): ZStream[Any, IOException, URI] =
    ZStream
      .fromZIO(ZIO.attempt(Paths.get(dirURI)).refineToOrDie[IOException])
      .flatMap(dirPath => tempPathIn(dirPath))
      .flatMap(path => ZStream.fromZIO(ZIO.attempt(path.toUri).refineToOrDie[IOException]))

  final def writeFile(file: => File)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(file.toPath).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

  final def writeFileName(name: => String)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(Paths.get(name)).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

  def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit]

  final def writeURI(uri: => URI)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      path <- ZSink.fromZIO(ZIO.attempt(Paths.get(uri)).refineToOrDie[IOException])
      r    <- writePath(path)
    } yield r

}
