package zio.nio.connect

import zio.nio.charset.Charset
import zio.{Chunk, Scope, Trace, ZIO, ZLayer}
import zio.nio.file.{FileSystem, Path, Files => ZFiles}
import zio.stream.ZStream

import java.io.IOException
import java.nio.file.{CopyOption, LinkOption, OpenOption}
import java.nio.file.attribute.FileAttribute
import java.nio.file.{Path => JPath, Files => JFiles}

trait Files {

  def list(path: JPath)(implicit trace: Trace): ZStream[Any, IOException, JPath]

  def createTempFileInScoped(
    dir: JPath,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, JPath]

  def notExists(path: JPath, linkOptions: LinkOption*)(implicit trace: Trace): ZIO[Any, Nothing, Boolean]

  def move(source: JPath, target: JPath, copyOptions: CopyOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def delete(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeLines(
    path: JPath,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  )(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeBytes(path: JPath, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def size(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Long]

}

object Files {

  def createTempFileInScoped(
    dir: JPath,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  ): ZIO[Scope with Files, IOException, JPath] =
    ZIO.serviceWithZIO[Files] { service =>
      service.createTempFileInScoped(dir, suffix, prefix, fileAttributes)
    }

  def notExists(path: JPath, linkOptions: LinkOption*): ZIO[Files, Nothing, Boolean] =
    ZIO.environmentWithZIO(_.get.notExists(path, linkOptions: _*))

  def list(path: JPath): ZStream[Files, IOException, JPath] =
    ZStream.environmentWithStream(_.get.list(path))

  def move(
    source: JPath,
    target: JPath,
    copyOptions: CopyOption*
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.move(source, target, copyOptions: _*))

  def delete(path: JPath)(implicit trace: Trace): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.delete(path))

  def writeLines(
    path: JPath,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeLines(path, lines, charset, openOptions))

  def writeBytes(
    path: JPath,
    bytes: Chunk[Byte],
    openOptions: OpenOption*
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeBytes(path, bytes, openOptions: _*))

  def size(path: Path)(implicit trace: Trace): ZIO[Files, IOException, Long] =
    ZIO.environmentWithZIO(_.get.size(path.javaPath))

  val live: ZLayer[Any, Nothing, Files] = ZLayer.succeed(new Files {
    override def createTempFileInScoped(
      dir: JPath,
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, JPath] =
      ZFiles.createTempFileInScoped(Path.fromJava(dir), suffix, prefix, fileAttributes).map(_.javaPath)

    override def notExists(path: JPath, linkOptions: LinkOption*)(implicit
      trace: Trace
    ): ZIO[Any, Nothing, Boolean] =
      ZFiles.notExists(Path.fromJava(path), linkOptions: _*)

    override def list(path: JPath)(implicit trace: Trace): ZStream[Any, IOException, JPath] =
      ZFiles.list(Path.fromJava(path)).map(_.javaPath)

    override def move(source: JPath, target: JPath, copyOptions: CopyOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.move(Path.fromJava(source), Path.fromJava(target), copyOptions: _*)

    override def delete(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Unit] =
      ZFiles.delete(Path.fromJava(path))

    override def writeLines(
      path: JPath,
      lines: Iterable[CharSequence],
      charset: Charset,
      openOptions: Set[OpenOption]
    )(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeLines(Path.fromJava(path), lines, charset, openOptions)

    override def writeBytes(path: JPath, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeBytes(Path.fromJava(path), bytes, openOptions: _*)

    override def size(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Long] =
      ZFiles.size(Path.fromJava(path))

  })

  val inMemory: ZLayer[FileSystem, Nothing, Files] =
    ZLayer.fromZIO(
      for {
        fs                <- ZIO.service[FileSystem]
        jimfsPathClassName = "com.google.common.jimfs.JimfsPath"
        files = new Files {
                  private def validatePath(path: JPath): ZIO[Any, Nothing, Any] =
                    ZIO
                      .die(
                        new RuntimeException(
                          s"Only $jimfsPathClassName are accepted in the inMemoryLayer. Instead was provided: ${path.getClass.getName}"
                        )
                      )
                      .unless(path.getClass.getName.equals(jimfsPathClassName))

                  override def list(path: JPath)(implicit
                    trace: Trace
                  ): ZStream[Any, IOException, JPath] =
                    path match {
                      case a if a.getClass.getName.equals(jimfsPathClassName) =>
                        ZStream.fromJavaStreamZIO(ZIO.attempt(JFiles.list(path))).refineOrDie { case a: IOException =>
                          a
                        }
                      case a =>
                        ZStream.die(
                          new RuntimeException(
                            s"Only $jimfsPathClassName are accepted in the inMemoryLayer. Instead was provided: ${a.getClass.getName}"
                          )
                        )
                    }

                  override def createTempFileInScoped(
                    dir: JPath,
                    suffix: String,
                    prefix: Option[String],
                    fileAttributes: Iterable[FileAttribute[_]]
                  )(implicit trace: Trace): ZIO[Scope, IOException, JPath] =
                    for {
                      _ <- validatePath(dir)
                      r <- ZFiles
                             .createTempFileInScoped(Path.fromJava(dir), suffix, prefix, fileAttributes)
                             .map(_.javaPath)
                    } yield r

                  override def notExists(path: JPath, linkOptions: LinkOption*)(implicit
                    trace: Trace
                  ): ZIO[Any, Nothing, Boolean] =
                    for {
                      _ <- validatePath(path)
                      r <- ZIO.attempt(JFiles.notExists(path)).orDie
                    } yield r

                  override def move(source: JPath, target: JPath, copyOptions: CopyOption*)(implicit
                    trace: Trace
                  ): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- validatePath(source)
                      _ <- validatePath(target)
                      _ <- ZFiles.move(Path.fromJava(source), Path.fromJava(target), copyOptions: _*)
                    } yield ()

                  override def delete(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- validatePath(path)
                      _ <- ZIO.attempt(JFiles.delete(path)).refineToOrDie[IOException]
                    } yield ()

                  override def writeLines(
                    path: JPath,
                    lines: Iterable[CharSequence],
                    charset: Charset,
                    openOptions: Set[OpenOption]
                  )(implicit trace: Trace): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- validatePath(path)
                      _ <- ZFiles.writeLines(Path.fromJava(path), lines, charset, openOptions)
                    } yield ()

                  override def writeBytes(path: JPath, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
                    trace: Trace
                  ): ZIO[Any, IOException, Unit] =
                    for {
                      _ <- validatePath(path)
                      _ <- ZFiles.writeBytes(Path.fromJava(path), bytes, openOptions: _*)
                    } yield ()

                  override def size(path: JPath)(implicit trace: Trace): ZIO[Any, IOException, Long] =
                    for {
                      _ <- validatePath(path)
                      r <- ZIO.attempt(JFiles.size(path)).orDie
                    } yield r

                }
      } yield files
    )

}
