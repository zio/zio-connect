package zio.connect.testkit.file

import zio.{Chunk, Duration, Ref, Trace, ZIO}
import zio.connect.file.FileConnector
import zio.connect.testkit.file.TestFileConnector._
import zio.nio.file.Path
import zio.stream.{ZSink, ZStream}

import java.io.IOException

//todo - once the testkit versions for FileSystem, Files and WatchService are created
//I think I can greatly simplify this by using them instead
case class TestFileConnector(fileSystem: Ref[Map[Path, File]]) extends FileConnector {

  override def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrap(for {
      dirFile  <- getFile(dir)
      children <- fileSystem.get.map(m => Chunk.fromIterable(m.filter(_._1.parent.contains(dir)).values.map(_.path)))
      r = dirFile match {
            case TestFileConnector.Dir(_)          => ZStream.fromChunk(children)
            case TestFileConnector.NonDir(path, _) => ZStream.fail(new IOException(s"$path is not a directory"))
          }
    } yield r)

  override def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ???
//    ZStream.unwrap(for {
//      f <- getFile(file)
//      r = f match {
//            case TestFileConnector.Dir(path, _)       => ZStream.fail(new IOException(s"$path is a directory"))
//            case TestFileConnector.NonDir(_, content) => ZStream.fromChunk(content)
//          }
//    } yield r)

  override def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    readFile(file)

  override def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] = readFile(file)

  override def writeFile(file: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] = ???
//    ZSink.unwrap(for {
//      fileOp       <- findFile(file)
//      overwriteRef <- Ref.make(fileOp.isDefined)
//      _ <- createNonDir(NonDir(file)).orDie
//             .whenZIO(findFile(file).map(_.isEmpty))
//      f <- getFile(file)
//      r = f match {
//            case TestFileConnector.Dir(path, _) => ZSink.fail(new IOException(s"$path is a directory"))
//            case TestFileConnector.NonDir(_, content) =>
//              ZSink.foreach[Any, IOException, Byte] { input =>
//                for {
//                  overwrite <- overwriteRef.get
//                  _ <- if (overwrite)
//                         fileSystem.update(m => m.updated(file, TestFileConnector.NonDir(file, Chunk(input))))
//                       else
//                         overwriteRef.set(false) *> fileSystem.update(m =>
//                           m.updated(file, TestFileConnector.NonDir(file, content ++ input))
//                         )
//                } yield ()
//              }
//          }
//    } yield r)

  override def deleteFile(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(f => removeFile(f))

  override def moveFile(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ???
//    ZSink.foreach { file =>
//      for {
//        f <- getFile(file)
//        _ <- f match {
//               case NonDir(path, content) =>
//                 removeFile(path) *> createNonDir(NonDir(locator(path), content))
//               case Dir(path, children) => ???
//             }
//      } yield ()
//    }

  private def removeFile(file: Path) = ???
//    fileSystem.getAndUpdate { m =>
//      val others = m.values.collect { case a @ Dir(_, children) if children.contains(file) => a.path }.toList
//      m.removedAll(others.appended(file))
//    }

  private def getFile(path: Path) =
    findFile(path)
      .flatMap(a => ZIO.fromOption(a))
      .mapError(_ => new IOException(s"File not found $path"))

  private def findFile(path: Path) =
    fileSystem.get
      .map(_.get(path))

//  private def createFile(file: File) =
//    findFile(file.path).flatMap {
//      case Some(_) => ZIO.fail(new Throwable(s"File ${file.path} already exists"))
//      case None =>
//        fileSystem
//          .update(m => m.updated(file.path, file))
//    }

}

object TestFileConnector {

  sealed trait File {
    def path: Path
  }

  final case class NonDir(path: Path, content: Chunk[Byte] = Chunk.empty) extends File
  final case class Dir(path: Path)                                        extends File

}
