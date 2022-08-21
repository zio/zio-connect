package zio.connect.testkit.file

import zio.{ZIO, ZIOAppDefault}
import com.google.common.jimfs.Configuration
import com.google.common.jimfs.Jimfs
import zio.nio.file.{Files, Path}

import java.nio.channels.FileChannel
import java.nio.file.{OpenOption, StandardOpenOption}

object Tests extends ZIOAppDefault {

  def run =
    for {
      fs        <- ZIO.attempt(Jimfs.newFileSystem(Configuration.forCurrentPlatform()))
      dir       <- ZIO.attempt(fs.getPath("/tmp"))
      _         <- ZIO.attempt(java.nio.file.Files.createDirectory(dir))
//      fileInDir <- java.nio.file.Files.createTempFile(dir, "test")
//      _         <- ZIO.debug(fileInDir)
//      _          = java.nio.file.Files.newBufferedReader(fileInDir.toFile.toPath)
//      _          = java.nio.file.Files.newByteChannel(fileInDir.toFile.toPath, Seq.empty: _*)
//      channel    = java.nio.channels.FileChannel.open(fileInDir.toFile.toPath, Seq(StandardOpenOption.READ): _*)
    } yield ()

  def run2 =
    for {
      fs        <- ZIO.attempt(Jimfs.newFileSystem(Configuration.forCurrentPlatform()))
      dir       <- ZIO.attempt(fs.getPath("/tmp"))
      zDir       = Path.fromJava(dir)
      _         <- Files.createDirectory(zDir)
      fileInDir <- Files.createTempFileInScoped(zDir, "test")
      _         <- ZIO.debug(fileInDir)
      _         <- Files.exists(fileInDir).tap(a => ZIO.debug(s"before delete $a"))
      _         <- Files.delete(fileInDir)
      _         <- Files.exists(fileInDir).tap(a => ZIO.debug(s"after $a"))

      file2 <- ZIO.attempt(fs.getPath("aaa"))
      _     <- ZIO.attempt(java.nio.file.Files.createFile(file2))
      fc1   <- ZIO.attempt(FileChannel.open(file2, Seq.empty[OpenOption]: _*))
      _     <- ZIO.attempt(fc1.close())
      _     <- ZIO.attempt(java.nio.file.Files.delete(file2))
      _     <- ZIO.attempt(java.nio.file.Files.exists(file2)).tap(a => ZIO.debug(a))
      _ <- ZIO.attempt(
             FileChannel.open(
               file2,
               StandardOpenOption
                 .values()
                 .filter(a =>
                   List(StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
                     .contains(a)
                 ): _*
             )
           )
      _ <- ZIO.attempt(java.nio.file.Files.exists(file2)).tap(a => ZIO.debug(a))
    } yield ()

}
