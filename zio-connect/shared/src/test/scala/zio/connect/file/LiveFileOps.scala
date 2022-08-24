package zio.connect.file

import zio.{Scope, ZIO, ZLayer}
import zio.nio.file.{Path, Files => ZFiles}

import java.nio.file.{Paths, Files => JFiles, Path => JPath}
import java.util.UUID

object LiveFileOps {

  val layer = ZLayer.succeed(
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
