package zio.connect.file

import zio.nio.file.{Path, Files => ZFiles}
import zio.{Scope, ZIO, ZLayer}

import java.nio.file.{Paths, Files => JFiles, Path => JPath}
import java.util.UUID

trait FileOps {
  def tempFileScoped: ZIO[Scope, Throwable, JPath]
  def tempDirScoped: ZIO[Scope, Throwable, JPath]
  def tempFileInDirScoped(dir: JPath): ZIO[Scope, Throwable, JPath]
  def getPath(first: String, more: String*): ZIO[Any, Throwable, JPath]
}

object FileOps {

  def tempFileScoped: ZIO[Scope with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.tempFileScoped)

  def tempDirScoped: ZIO[Scope with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.tempDirScoped)

  def tempFileInDirScoped(dir: JPath): ZIO[Scope with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.tempFileInDirScoped(dir))

  def getPath(first: String, more: String*): ZIO[Any with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.getPath(first, more: _*))

  val live = ZLayer.succeed(
    new FileOps {
      override def tempFileScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt(JFiles.createTempFile(UUID.randomUUID().toString, ""))
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)

      override def tempDirScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt(JFiles.createTempDirectory(UUID.randomUUID().toString))
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)

      override def tempFileInDirScoped(dir: JPath): ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(ZIO.attempt(JFiles.createTempFile(dir, "", "")))(p =>
          ZIO.attempt(JFiles.deleteIfExists(p)).orDie
        )

      override def getPath(first: String, more: String*): ZIO[Any, Throwable, JPath] =
        ZIO.attempt(Paths.get(first, more: _*))
    }
  )

}
