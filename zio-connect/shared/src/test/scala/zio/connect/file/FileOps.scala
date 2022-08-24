package zio.connect.file

import zio.{Scope, ZIO, ZLayer}
import zio.nio.file.{Path, Files => ZFiles}
import java.nio.file.{Files => JFiles, Path => JPath}

import java.util.UUID

trait FileOps {
  def tempFileScoped: ZIO[Scope, Throwable, JPath]
  def tempDirScoped: ZIO[Scope, Throwable, JPath]
}

object FileOps {

  def tempFileScoped: ZIO[Scope with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.tempFileScoped)

  def tempDirScoped: ZIO[Scope with FileOps, Throwable, JPath] =
    ZIO.serviceWithZIO[FileOps](_.tempDirScoped)

  val liveFileOps = ZLayer.succeed(
    new FileOps {
      override def tempFileScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt(JFiles.createTempFile(UUID.randomUUID().toString, ""))
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)

      override def tempDirScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt(JFiles.createTempDirectory(UUID.randomUUID().toString))
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)
    }
  )

  val inMemoryFileOps = ZLayer.fromZIO(
    for {
      fs <- ZIO.service[java.nio.file.FileSystem]
    } yield new FileOps {
      override def tempFileScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt {
            val tmpPath = fs.getPath(UUID.randomUUID().toString)
            JFiles.createFile(tmpPath)
          }
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)

      override def tempDirScoped: ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(
          ZIO.attempt {
            val tmpPath = fs.getPath(UUID.randomUUID().toString)
            JFiles.createDirectory(tmpPath)
          }
        )(p => ZFiles.deleteIfExists(Path.fromJava(p)).orDie)
    }
  )

}
