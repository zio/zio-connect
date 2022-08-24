package zio.connect.file

import zio.{Scope, ZIO}

import java.nio.file.Path

trait FileOps {
  def tempFileScoped: ZIO[Scope, Throwable, Path]
  def tempDirScoped: ZIO[Scope, Throwable, Path]
  def tempFileInDirScoped(dir: Path): ZIO[Scope, Throwable, Path]
  def getPath(first: String, more: String*): ZIO[Any, Throwable, Path]
}

object FileOps {

  def tempFileScoped: ZIO[Scope with FileOps, Throwable, Path] =
    ZIO.serviceWithZIO[FileOps](_.tempFileScoped)

  def tempDirScoped: ZIO[Scope with FileOps, Throwable, Path] =
    ZIO.serviceWithZIO[FileOps](_.tempDirScoped)

  def tempFileInDirScoped(dir: Path): ZIO[Scope with FileOps, Throwable, Path] =
    ZIO.serviceWithZIO[FileOps](_.tempFileInDirScoped(dir))

  def getPath(first: String, more: String*): ZIO[Any with FileOps, Throwable, Path] =
    ZIO.serviceWithZIO[FileOps](_.getPath(first, more: _*))

}
