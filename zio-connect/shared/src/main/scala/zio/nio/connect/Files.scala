package zio.nio.connect

import zio.nio.charset.Charset
import zio.{Chunk, Scope, Trace, ZIO, ZLayer}
import zio.nio.file.{Path, Files => ZFiles}
import zio.stream.ZStream

import java.io.IOException
import java.nio.file.{CopyOption, LinkOption, OpenOption}
import java.nio.file.attribute.FileAttribute

trait Files {

  def list(path: Path)(implicit trace: Trace): ZStream[Any, IOException, Path]

  def createTempFileInScoped(
    dir: Path,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, Path]

  def notExists(path: Path, linkOptions: LinkOption*)(implicit trace: Trace): ZIO[Any, Nothing, Boolean]

  def move(source: Path, target: Path, copyOptions: CopyOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def delete(path: Path)(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeLines(
    path: Path,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  )(implicit trace: Trace): ZIO[Any, IOException, Unit]

  def writeBytes(path: Path, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
    trace: Trace
  ): ZIO[Any, IOException, Unit]

  def createTempFileScoped(
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, Path]

  def createTempDirectoryScoped(
    prefix: Option[String],
    fileAttributes: Iterable[FileAttribute[_]]
  )(implicit trace: Trace): ZIO[Scope, IOException, Path]

}

object Files {

  def createTempFileInScoped(
    dir: Path,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  ): ZIO[Scope with Files, IOException, Path] =
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

  def createTempDirectoryScoped(
    prefix: Option[String],
    fileAttributes: Iterable[FileAttribute[_]]
  ): ZIO[Scope with Files, IOException, Path] =
    ZIO.serviceWithZIO[Files] { service =>
      service.createTempDirectoryScoped(prefix, fileAttributes)
    }

  def notExists(path: Path, linkOptions: LinkOption*): ZIO[Files, Nothing, Boolean] =
    ZIO.environmentWithZIO(_.get.notExists(path, linkOptions: _*))

  def list(path: Path): ZStream[Files, IOException, Path] =
    ZStream.environmentWithStream(_.get.list(path))

  def move(source: Path, target: Path, copyOptions: CopyOption*): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.move(source, target, copyOptions: _*))

  def delete(path: Path)(implicit trace: Trace): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.delete(path))

  def writeLines(
    path: Path,
    lines: Iterable[CharSequence],
    charset: Charset = Charset.Standard.utf8,
    openOptions: Set[OpenOption] = Set.empty
  ): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeLines(path, lines, charset, openOptions))

  def writeBytes(path: Path, bytes: Chunk[Byte], openOptions: OpenOption*): ZIO[Files, IOException, Unit] =
    ZIO.environmentWithZIO(_.get.writeBytes(path, bytes, openOptions: _*))

  val live: ZLayer[Any, Nothing, Files] = ZLayer.succeed(new Files {
    override def createTempFileInScoped(
      dir: Path,
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
      ZFiles.createTempFileInScoped(dir, suffix, prefix, fileAttributes)

    override def notExists(path: Path, linkOptions: LinkOption*)(implicit trace: Trace): ZIO[Any, Nothing, Boolean] =
      ZFiles.notExists(path, linkOptions: _*)

    override def list(path: Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
      ZFiles.list(path)

    override def move(source: Path, target: Path, copyOptions: CopyOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.move(source, target, copyOptions: _*)

    override def delete(path: Path)(implicit trace: Trace): ZIO[Any, IOException, Unit] =
      ZFiles.delete(path)

    override def writeLines(path: Path, lines: Iterable[CharSequence], charset: Charset, openOptions: Set[OpenOption])(
      implicit trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeLines(path, lines, charset, openOptions)

    override def writeBytes(path: Path, bytes: Chunk[Byte], openOptions: OpenOption*)(implicit
      trace: Trace
    ): ZIO[Any, IOException, Unit] =
      ZFiles.writeBytes(path, bytes, openOptions: _*)

    override def createTempFileScoped(
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
      ZFiles.createTempFileScoped(suffix, prefix, fileAttributes)

    override def createTempDirectoryScoped(
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
      ZFiles.createTempDirectoryScoped(prefix, fileAttributes)
  })

}
