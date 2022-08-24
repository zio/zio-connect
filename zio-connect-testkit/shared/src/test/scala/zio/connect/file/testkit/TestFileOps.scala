package zio.connect.file.testkit

import zio.connect.file.FileOps
import zio.nio.file.{Path, Files => ZFiles}
import zio.{Scope, ZIO, ZLayer}

import java.nio.file.{Files => JFiles, Path => JPath}
import java.util.UUID

object TestFileOps {

  val layerWithCustomFileSystem = ZLayer.fromZIO(
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

      override def tempFileInDirScoped(dir: JPath): ZIO[Scope, Throwable, JPath] =
        ZIO.acquireRelease(ZIO.attempt(JFiles.createTempFile(dir, "", "")))(p =>
          ZIO.attempt(JFiles.deleteIfExists(p)).orDie
        )

      override def getPath(first: String, more: String*): ZIO[Any, Throwable, JPath] =
        ZIO.attempt(fs.getPath(first, more: _*))

    }
  )

}
