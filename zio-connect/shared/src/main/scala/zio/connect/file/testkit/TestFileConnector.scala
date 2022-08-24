package zio.connect.file.testkit

import com.google.common.jimfs.{Configuration, Jimfs}
import zio.connect.file.{FileConnector, LiveFileConnector}
import zio.nio.file.WatchService
import zio.stream.{ZSink, ZStream}
import zio.{Duration, Trace, ZIO, ZLayer}

import java.io.IOException
import java.nio.file.{FileSystem, Path}

case class TestFileConnector(fs: FileSystem, fileConnector: FileConnector) extends FileConnector {

  private val pathClass = "com.google.common.jimfs.JimfsPath"
  override def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    if (dir.getClass.getName.equals(pathClass)) {
      fileConnector.listDir(dir)
    } else {
      val inMemoryPath = fs.getPath(dir.getFileName.toString)
      fileConnector.listDir(inMemoryPath)
    }

  override def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    if (file.getClass.getName.equals(pathClass)) {
      fileConnector.readFile(file)
    } else {
      val inMemoryPath = fs.getPath(file.getFileName.toString)
      fileConnector.readFile(inMemoryPath)
    }

  override def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    if (file.getClass.getName.equals(pathClass)) {
      fileConnector.tailFile(file, freq)
    } else {
      val inMemoryPath = fs.getPath(file.getFileName.toString)
      fileConnector.tailFile(inMemoryPath, freq)
    }

  override def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    if (file.getClass.getName.equals(pathClass)) {
      fileConnector.tailFileUsingWatchService(file, freq)
    } else {
      val inMemoryPath = fs.getPath(file.getFileName.toString)
      fileConnector.tailFileUsingWatchService(inMemoryPath, freq)
    }

  override def writeFile(file: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    if (file.getClass.getName.equals(pathClass)) {
      fileConnector.writeFile(file)
    } else {
      val inMemoryPath = fs.getPath(file.getFileName.toString)
      fileConnector.writeFile(inMemoryPath)
    }

  override def deleteFile(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    fileConnector.deleteFile.contramap { file =>
      if (file.getClass.getName.equals(pathClass)) {
        file
      } else {
        fs.getPath(file.getFileName.toString)
      }
    }

  override def moveFile(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    fileConnector.moveFile(locator).contramap { file =>
      if (file.getClass.getName.equals(pathClass)) {
        file
      } else {
        fs.getPath(file.getFileName.toString)
      }
    }
}

object TestFileConnector {

  def layer: ZLayer[Any, Nothing, FileConnector] =
    ZLayer.fromZIO {
      for {
        fs           <- ZIO.attempt(Jimfs.newFileSystem(Configuration.forCurrentPlatform())).orDie
        watchService <- ZIO.attempt(WatchService.fromJava(fs.newWatchService())).orDie
      } yield new TestFileConnector(fs, LiveFileConnector(watchService))
    }

  private[testkit] val layerWithCustomFileSystem: ZLayer[java.nio.file.FileSystem, Nothing, FileConnector] =
    ZLayer.fromZIO {
      val fileSystemClass = "com.google.common.jimfs.JimfsFileSystem"
      for {
        fs <- ZIO.service[java.nio.file.FileSystem]
        _ <-
          ZIO.when(!fs.getClass.getName.equals(fileSystemClass))(
            ZIO.die(new RuntimeException("Not an inMemory fileSystem"))
          )
        watchService <- ZIO.attempt(WatchService.fromJava(fs.newWatchService())).orDie
      } yield new TestFileConnector(fs, LiveFileConnector(watchService))
    }

}
