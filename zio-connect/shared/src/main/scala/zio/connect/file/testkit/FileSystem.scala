package zio.connect.file.testkit

import zio.stm.{STM, TRef, ZSTM}
import zio.{Chunk, ZIO}

import java.io.{File, FileNotFoundException, IOException}
import java.nio.file.{DirectoryNotEmptyException, Path, Paths}
import java.util.UUID

//TODO: inline all of this in TestFileConnectorSpec
sealed trait FileSystemNode {
  def path: Path
  def replacePath(newPath: Path): FileSystemNode
}

object FileSystemNode {
  final case class Dir(path: Path) extends FileSystemNode {
    override def replacePath(newPath: Path): FileSystemNode = Dir(newPath)
  }
  final case class File(path: Path, content: Chunk[Byte]) extends FileSystemNode {
    override def replacePath(newPath: Path): FileSystemNode = File(newPath, content)
  }
}

//TODO: inline all of this in TestFileConnectorSpec
final case class TestFileSystem(map: TRef[Map[Path, FileSystemNode]]) {

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

  def movePath(sourcePath: Path, destinationPath: Path): ZIO[Any, IOException, Unit] =
    STM.atomically {
      movePathSTM(sourcePath, destinationPath)
    }

  private def movePathSTM(sourcePath: Path, destinationPath: Path): ZSTM[Any, IOException, Unit] =
    for {
      sourceFile        <- getFile(sourcePath)
      fileAlreadyExists <- findFileSTM(destinationPath).map(_.isDefined)
      _ <-
        STM.when(fileAlreadyExists)(ZSTM.fail(new IOException(s"File already exists at destination $destinationPath")))
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

}
