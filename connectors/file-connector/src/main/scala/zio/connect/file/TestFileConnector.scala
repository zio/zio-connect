package zio.connect.file

import zio.connect.file.TestFileConnector.TestFileSystem
import zio.stm.{STM, TRef, ZSTM}
import zio.stream.{ZSink, ZStream}
import zio.{Chunk, Duration, Queue, Ref, Schedule, Trace, ZIO, ZLayer}

import java.io.{File, FileNotFoundException, IOException}
import java.nio.file.{DirectoryNotEmptyException, Files, Path, Paths}
import java.util.UUID

private[file] final case class TestFileConnector(fs: TestFileSystem) extends FileConnector {

  override def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path => fs.delete(path))

  override def deletePathRecursively(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path => fs.deleteRecursively(path))

  override def existsPath(implicit trace: Trace): ZSink[Any, IOException, Path, Path, Boolean] =
    ZSink
      .take[Path](1)
      .map(_.headOption)
      .mapZIO {
        case Some(p) => ZIO.attempt(Files.exists(p)).refineToOrDie[IOException]
        case None    => ZIO.succeed(false)
      }

  override def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrap(fs.list(path).map(a => ZStream.fromChunk(a)))

  override def movePathZIO(
    locator: Path => ZIO[Any, IOException, Path]
  )(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach { oPath =>
      fs.movePath(oPath, locator(oPath))
    }

  override def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(fs.getContent(path).map(a => ZStream.fromChunk(a)))

  override def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] = {

    def push(index: Ref[Int], queue: Queue[Byte]): ZIO[Any, IOException, Unit] =
      for {
        content   <- fs.getContent(path)
        idx       <- index.get
        newContent = content.drop(idx)
        _         <- queue.offerAll(newContent)
        newIndex   = content.length
        _         <- index.set(newIndex)

      } yield ()

    ZStream.unwrap(
      for {
        queue <- Queue.unbounded[Byte]
        index <- Ref.make(0)
        _ <-
          push(index, queue).repeat[Any, (Long, Long)](Schedule.recurs(10) && Schedule.spaced(Duration.fromMillis(100)))
      } yield ZStream.fromQueue(queue)
    )

  }

  override def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] = tailPath(path, freq)

  override def tempPath(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO.acquireRelease(fs.tempPath)(path => fs.delete(path).orDie).map(path => ZStream.fromZIO(ZIO.succeed(path)))
    )

  override def tempPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(fs.tempPathIn(dirPath))(path => fs.delete(path).orDie)
        .map(path => ZStream.fromZIO(ZIO.succeed(path)))
    )

  override def tempDirPath(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(fs.tempDirPath)(path => fs.deleteRecursively(path).orDie)
        .map(path => ZStream.fromZIO(ZIO.succeed(path)))
    )

  override def tempDirPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(fs.tempDirPathIn(dirPath))(path => fs.deleteRecursively(path).orDie)
        .map(path => ZStream.fromZIO(ZIO.succeed(path)))
    )

  override def writePath(path: => Path)(implicit trace: Trace): ZSink[Any, IOException, Byte, Nothing, Unit] =
    for {
      _ <- ZSink.fromZIO(fs.removeContentIfExists(path))
      _ <- ZSink.foreachChunk(bytes => fs.write(path, bytes))
    } yield ()

}

object TestFileConnector {

  val layer: ZLayer[Any, Nothing, FileConnector] = ZLayer.fromZIO(
    STM.atomically {
      for {
        a <- TRef.make(Map.empty[Path, FileSystemNode])
      } yield TestFileConnector(TestFileSystem(a))
    }
  )

  private[file] sealed trait FileSystemNode {
    def path: Path

    def replacePath(newPath: Path): FileSystemNode
  }

  private[file] object FileSystemNode {
    final case class Dir(path: Path) extends FileSystemNode {
      override def replacePath(newPath: Path): FileSystemNode = Dir(newPath)
    }

    final case class File(path: Path, content: Chunk[Byte]) extends FileSystemNode {
      override def replacePath(newPath: Path): FileSystemNode = File(newPath, content)
    }
  }

  private[file] final case class TestFileSystem(map: TRef[Map[Path, FileSystemNode]]) {

    def delete(path: Path): ZIO[Any, IOException, Unit] =
      STM.atomically {
        deleteSTM(path)
      }

    private def deleteSTM(path: Path): ZSTM[Any, IOException, Unit] =
      for {
        file <- findFileSTM(path)
        _ <- file match {
               case Some(value) =>
                 value match {
                   case FileSystemNode.Dir(_) =>
                     for {
                       children <- getChildren(path)
                       _ <- if (children.isEmpty)
                              map.update(m => m - path)
                            else ZSTM.fail(new DirectoryNotEmptyException(s"$path"))
                     } yield ()
                   case FileSystemNode.File(_, _) => map.update(m => m - path)
                 }
               case None => ZSTM.unit
             }
      } yield ()

    private def deleteRecursivelySTM(path: Path): ZSTM[Any, IOException, Unit] =
      for {
        file     <- findFileSTM(path)
        children <- getChildren(path)
        all       = children ++ Chunk.fromIterable(file.toList)
        _        <- map.update(m => m -- all.map(_.path))
      } yield ()

    def deleteRecursively(path: Path): ZIO[Any, IOException, Unit] =
      STM.atomically {
        deleteRecursivelySTM(path)
      }

    def exists(path: Path): ZIO[Any, Nothing, Boolean] =
      STM.atomically {
        for {
          files <- map.get
          r     <- ZSTM.succeed(files.contains(path))
        } yield r
      }

    private def findFileSTM(path: Path): ZSTM[Any, Nothing, Option[FileSystemNode]] =
      map.get.map(_.get(path))

    private def getFile(path: Path): ZSTM[Any, IOException, FileSystemNode] =
      for {
        file <- findFileSTM(path)
        r <- file match {
               case Some(p) => STM.succeed(p)
               case None    => STM.fail(new FileNotFoundException(s"$path"))
             }
      } yield r

    private def getChildren(path: Path): ZSTM[Any, Nothing, Chunk[FileSystemNode]] =
      for {
        files <- map.get
        children =
          Chunk
            .fromIterable(files.filter(_._1.startsWith(path)))
            .filterNot(_._1 == path)
            .filter { a =>
              !a._1.toString.replace(path.toString + File.separator, "").contains(File.separator)
            }
            .map(_._2)
      } yield children

    def getContent(path: Path): ZIO[Any, IOException, Chunk[Byte]] =
      STM.atomically {
        for {
          file <- findFileSTM(path)
          r <- file match {
                 case Some(p) =>
                   p match {
                     case FileSystemNode.Dir(path)        => STM.fail(new IOException(s"$path is a directory"))
                     case FileSystemNode.File(_, content) => STM.succeed(content)
                   }
                 case None => STM.fail(new FileNotFoundException(s"$path"))
               }
        } yield r
      }

    def list(path: Path): ZIO[Any, IOException, Chunk[Path]] =
      STM.atomically {
        for {
          fileExists <- findFileSTM(path).map(_.isDefined)
          _          <- ZSTM.when(!fileExists)(ZSTM.fail(new FileNotFoundException(s"$path")))
          children   <- getChildren(path).map(_.map(_.path))
        } yield children
      }

    private def listFileAndAllDescendants(path: Path): ZSTM[Any, Nothing, Chunk[FileSystemNode]] =
      for {
        files   <- map.get
        children = Chunk.fromIterable(files.filter(_._1.startsWith(path))).map(_._2)
      } yield children

    def movePath(sourcePath: Path, destinationPath: ZIO[Any, IOException, Path]): ZIO[Any, IOException, Unit] =
      for {
        dest <- destinationPath
        r <- STM.atomically {
               movePathSTM(sourcePath, dest)
             }
      } yield r

    private def movePathSTM(
      sourcePath: Path,
      destinationPath: Path
    ): ZSTM[Any, IOException, Unit] =
      for {
        sourceFile        <- getFile(sourcePath)
        fileAlreadyExists <- findFileSTM(destinationPath).map(_.isDefined)
        _ <-
          STM.when(fileAlreadyExists)(
            ZSTM.fail(new IOException(s"File already exists at destination $destinationPath"))
          )
        _ <- sourceFile match {
               case a: FileSystemNode.Dir =>
                 for {
                   fileAndAllDescendants <- listFileAndAllDescendants(a.path)
                   renamedFiles =
                     fileAndAllDescendants.map(f =>
                       f.replacePath(Paths.get(f.path.toString.replace(a.path.toString, destinationPath.toString)))
                     )
                   _ <- deleteRecursivelySTM(a.path)
                   _ <- map.update(m => m ++ renamedFiles.map(f => f.path -> f))
                 } yield ()
               case a: FileSystemNode.File =>
                 for {
                   newFile <- STM.succeed(a.replacePath(destinationPath))
                   _       <- map.update(m => m.updated(destinationPath, newFile))
                   _       <- deleteSTM(a.path)
                 } yield ()
             }
      } yield ()

    def removeContentIfExists(path: Path): ZIO[Any, IOException, Unit] =
      STM.atomically {
        for {
          file <- findFileSTM(path)
          r <- file match {
                 case Some(p) =>
                   p match {
                     case FileSystemNode.Dir(path) => ZSTM.fail(new IOException(s"$path is a directory"))
                     case FileSystemNode.File(_, _) =>
                       map.update(m => m.updated(path, FileSystemNode.File(path, Chunk.empty[Byte])))
                   }
                 case None => ZSTM.fail(new FileNotFoundException(s"$path"))
               }
        } yield r
      }

    def tempDirPath: ZIO[Any, Nothing, Path] =
      STM.atomically {
        for {
          tempPath <- ZSTM.attempt(Paths.get(UUID.randomUUID().toString)).orDie
          _        <- map.update(m => m.updated(tempPath, FileSystemNode.Dir(tempPath)))
        } yield tempPath
      }

    def tempDirPathIn(path: Path): ZIO[Any, Nothing, Path] =
      STM.atomically {
        for {
          file <- getFile(path).orDie
          r <- file match {
                 case FileSystemNode.Dir(_) =>
                   for {
                     tempPath <- ZSTM.succeed(Paths.get(path.toString, UUID.randomUUID().toString))
                     _        <- map.update(m => m.updated(tempPath, FileSystemNode.Dir(tempPath)))
                   } yield tempPath
                 case FileSystemNode.File(_, _) => STM.die(new IOException(s"$path is not a directory"))
               }
        } yield r
      }

    def tempPath: ZIO[Any, Nothing, Path] =
      STM.atomically {
        for {
          tempPath <- STM.attempt(Paths.get(UUID.randomUUID().toString)).orDie
          _        <- map.update(m => m.updated(tempPath, FileSystemNode.File(tempPath, Chunk.empty)))
        } yield tempPath
      }

    def tempPathIn(dir: Path): ZIO[Any, Nothing, Path] =
      STM.atomically {
        for {
          tempPath <- STM.attempt(Paths.get(dir.toString, UUID.randomUUID().toString)).orDie
          _        <- map.update(m => m.updated(tempPath, FileSystemNode.File(tempPath, Chunk.empty)))
        } yield tempPath
      }

    def write(path: Path, bytes: Chunk[Byte]): ZIO[Any, IOException, Unit] =
      STM.atomically {
        for {
          file <- findFileSTM(path)
          _ <- file match {
                 case Some(p) =>
                   p match {
                     case FileSystemNode.Dir(path) => ZSTM.fail(new IOException(s"$path is a directory"))
                     case FileSystemNode.File(path, content) =>
                       map.update(m => m.updated(path, FileSystemNode.File(path, content ++ bytes)))
                   }
                 case None =>
                   map.update(m => m.updated(path, FileSystemNode.File(path, bytes)))
               }
        } yield ()
      }

  }

}
