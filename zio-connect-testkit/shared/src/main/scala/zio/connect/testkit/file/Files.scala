package zio.connect.testkit.file

import zio.{Scope, Trace, ZIO}
import zio.nio.file.{Path, Files => ZFiles}

import java.io.IOException
import java.nio.file.attribute.FileAttribute

//todo - will add mirror operators here as necessary so I can eventually use this in FileConnectorSpec
//todo - also need a version of FileSystem and WatchService
trait Files {

  def createTempFileInScoped(
    dir: Path,
    suffix: String = ".tmp",
    prefix: Option[String] = None,
    fileAttributes: Iterable[FileAttribute[_]] = Nil
  )(implicit trace: Trace): ZIO[Scope, IOException, Path]

}

object Files {

  val live: Files = new Files {
    override def createTempFileInScoped(
      dir: Path,
      suffix: String,
      prefix: Option[String],
      fileAttributes: Iterable[FileAttribute[_]]
    )(implicit trace: Trace): ZIO[Scope, IOException, Path] =
      ZFiles.createTempFileInScoped(dir, suffix, prefix, fileAttributes)
  }

  //
  def inMemory : Files = ???

}
