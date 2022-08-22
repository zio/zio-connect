package zio.nio.connect

import zio.ZIO.attemptBlocking
import zio.nio.charset.Charset
import zio.{Chunk, Scope, Trace, ZIO, ZLayer}
import zio.nio.file.{FileSystem, Path, Files => ZFiles}
import zio.stream.ZStream

import java.io.IOException
import java.nio.file.{CopyOption, LinkOption, OpenOption}
import java.nio.file.attribute.FileAttribute
import java.nio.file.{Files => JFiles}
import java.util.UUID

trait Files {

  def list(path: java.nio.file.Path)(implicit trace: Trace): ZStream[Any, IOException, java.nio.file.Path]

  def createTempFileInScoped(
    dir: java.nio.file.Path,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, java.nio.file.Path]

  def notExists(path: java.nio.file.Path, linkOptions: LinkOption*)(implicit trace: Trace): ZIO[Any, Nothing, Boolean]

  def move(source: java.nio.file.Path, target: java.nio.file.Path, copyOptions: CopyOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def delete(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeLines(
    path: java.nio.file.Path,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  )(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeBytes(path: java.nio.file.Path, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def createTempFileScoped(
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, Path]

  def size(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Long]

  def deleteIfExists(path: Path)(implicit trace: Trace): ZIO[Any, IOException, Boolean]

}

object Files {

  def createTempFileInScoped(
    dir: java.nio.file.Path,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  ): ZIO[Scope with Files, IOException, java.nio.file.Path] =
    ZIO.serviceWithZIO[Files] { service =>
      service.createTempFileInScoped(dir, suffix, prefix, fileAttributes)
    }

  def createTempFileScoped(
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  ): ZIO[Scope with Files, IOException, Path] =
    ZIO.serviceWithZIO[Files] { service =>
      service.createTempFileScoped(suffix, prefix, fileAttributes)
    }


  def notExists(path: java.nio.file.Path, linkOptions: LinkOption*): ZIO[Files, Nothing, Boolean] =
    ZIO.environmentWithZIO(_.get.notExists(path, linkOptions: _*))

  def list(path: java.nio.file.Path): ZStream[Files, IOException, java.nio.file.Path] =
    ZStream.environmentWithStream(_.get.list(path))

  def move(
    source: java.nio.file.Path,
    target: java.nio.file.Path,
    copyOptions: CopyOption*
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.move(source, target, copyOptions: _*))

  def delete(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.delete(path))

  def writeLines(
    path: java.nio.file.Path,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeLines(path, lines, charset, openOptions))

  def writeBytes(
    path: java.nio.file.Path,
    bytes: Chunk[Byte],
    openOptions: OpenOption*
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeBytes(path, bytes, openOptions: _*))

  def size(path: Path)(implicit trace: Trace): ZIO[Files, IOException, Long] =
    ZIO.environmentWithZIO(_.get.size(path.javaPath))

  val live: ZLayer[Any, Nothing, Files] = ZLayer.succeed(new Files {
    override def createTempFileInScoped(
      dir: java.nio.file.Path,
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, java.nio.file.Path] =
      ZFiles.createTempFileInScoped(Path.fromJava(dir), suffix, prefix, fileAttributes).map(_.javaPath)

    override def notExists(path: java.nio.file.Path, linkOptions: LinkOption*)(implicit
      trace: Trace
    ): ZIO[Any, Nothing, Boolean] =
      ZFiles.notExists(Path.fromJava(path), linkOptions: _*)

    override def list(path: java.nio.file.Path)(implicit trace: Trace): ZStream[Any, IOException, java.nio.file.Path] =
      ZFiles.list(Path.fromJava(path)).map(_.javaPath)

    override def move(source: java.nio.file.Path, target: java.nio.file.Path, copyOptions: CopyOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.move(Path.fromJava(source), Path.fromJava(target), copyOptions: _*)

    override def delete(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Unit] =
      ZFiles.delete(Path.fromJava(path))

    override def writeLines(
      path: java.nio.file.Path,
      lines: Iterable[CharSequence],
      charset: Charset,
      openOptions: Set[OpenOption]
    )(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeLines(Path.fromJava(path), lines, charset, openOptions)

    override def writeBytes(path: java.nio.file.Path, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeBytes(Path.fromJava(path), bytes, openOptions: _*)

    override def createTempFileScoped(
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
      ZFiles.createTempFileScoped(suffix, prefix, fileAttributes)


    override def size(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Long] =
      ZFiles.size(Path.fromJava(path))

    override def deleteIfExists(path: Path)(implicit trace: Trace): ZIO[Any, IOException, Boolean] =
      ZFiles.deleteIfExists(path)
  })

  val inMemory: ZLayer[FileSystem, Nothing, Files] =
    ZLayer.fromZIO(
      for {
        fs                <- ZIO.service[FileSystem]
        jimfsPathClassName = "com.google.common.jimfs.JimfsPath"
        files = new Files {
                  override def list(path: java.nio.file.Path)(implicit
                    trace: Trace
                  ): ZStream[Any, IOException, java.nio.file.Path] =
                    path match {
                      case a if a.getClass.getName.equals(jimfsPathClassName) =>
                        ZStream.fromJavaStreamZIO(ZIO.attempt(java.nio.file.Files.list(path))).refineOrDie {
                          case a: IOException => a
                        }
                      case a =>
                        ZStream.die(
                          new RuntimeException(
                            s"Only $jimfsPathClassName are accepted in the inMemoryLayer. Instead was provided: ${a.getClass.getName}"
                          )
                        )
                    }

                  override def createTempFileInScoped(
                    dir: java.nio.file.Path,
                    suffix: String,
                    prefix: Option[String],
                    fileAttributes: Iterable[FileAttribute[_]]
                  )(implicit trace: Trace): ZIO[Scope, IOException, java.nio.file.Path] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(dir.getClass.getName.equals(jimfsPathClassName))
                      r <- ZFiles
                             .createTempFileInScoped(Path.fromJava(dir), suffix, prefix, fileAttributes)
                             .map(_.javaPath)
                    } yield r

                  override def notExists(path: java.nio.file.Path, linkOptions: LinkOption*)(implicit
                    trace: Trace
                  ): ZIO[Any, Nothing, Boolean] =
                    path match {
                      case a if a.getClass.getName.equals(jimfsPathClassName) =>
                        ZIO.attempt(java.nio.file.Files.notExists(path)).orDie
                      case _ => ZIO.die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                    }

                  override def move(source: java.nio.file.Path, target: java.nio.file.Path, copyOptions: CopyOption*)(
                    implicit trace: Trace
                  ): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(source.getClass.getName.equals(jimfsPathClassName))
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(target.getClass.getName.equals(jimfsPathClassName))
                      _ <- ZFiles.move(Path.fromJava(source), Path.fromJava(target), copyOptions: _*)
                    } yield ()

                  override def delete(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(path.getClass.getName.equals(jimfsPathClassName))
                      _ <- ZIO.attempt(java.nio.file.Files.delete(path)).refineToOrDie[IOException]
                    } yield ()

                  override def writeLines(
                    path: java.nio.file.Path,
                    lines: Iterable[CharSequence],
                    charset: Charset,
                    openOptions: Set[OpenOption]
                  )(implicit trace: Trace): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(path.getClass.getName.equals(jimfsPathClassName))
                      _ <- ZFiles.writeLines(Path.fromJava(path), lines, charset, openOptions)
                    } yield ()

                  override def writeBytes(path: java.nio.file.Path, bytes: Chunk[Byte], openOptions: OpenOption*)(
                    implicit trace: Trace
                  ): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(path.getClass.getName.equals(jimfsPathClassName))
                      _ <- ZFiles.writeBytes(Path.fromJava(path), bytes, openOptions: _*)
                    } yield ()

                  override def createTempFileScoped(
                    suffix: String,
                    prefix: Option[String],
                    fileAttributes: Iterable[FileAttribute[_]]
                  )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
                    ZIO.acquireRelease(createTempFile(suffix, prefix, fileAttributes))(release =
                      deleteIfExists(_).ignore
                    )

                  override def size(path: java.nio.file.Path)(implicit trace: Trace): ZIO[Any, IOException, Long] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(path.getClass.getName.equals(jimfsPathClassName))
                      r <- ZIO.attempt(java.nio.file.Files.size(path)).orDie
                    } yield r

                  override def deleteIfExists(path: Path)(implicit trace: Trace): ZIO[Any, IOException, Boolean] =
                    for {
                      _ <- ZIO
                             .die(new RuntimeException("Only JimfsPath are accepted in the inMemoryLayer"))
                             .unless(path.getClass.getName.equals(jimfsPathClassName))
                      r <- ZFiles.deleteIfExists(path)
                    } yield r

                  def createTempFile(
                    suffix: String = ".tmp",
                    prefix: Option[String],
                    fileAttributes: Iterable[FileAttribute[_]]
                  )(implicit trace: Trace): ZIO[Any, IOException, Path] =
                    ZIO.attempt {
                      val p = prefix.getOrElse("")

                      val fileName = s"$p${UUID.randomUUID().toString}$suffix"
                      val path     = fs.getPath(fileName).javaPath
                      path
                    }.flatMap(path => attemptBlocking(Path.fromJava(JFiles.createFile(path, fileAttributes.toSeq: _*))))
                      .refineToOrDie[IOException]
                }
      } yield files
    )

}
