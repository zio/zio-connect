package zio.connect.file.testkit

import zio.{Chunk, ZIO}
import zio.stm.{STM, TRef, ZSTM}

import java.io.{FileNotFoundException, IOException}
import java.nio.file.{DirectoryNotEmptyException, Path, Paths}
import java.util.UUID

sealed trait TKFile {
  def path: Path
}

object TKFile {
  final case class Dir(path: Path)  extends TKFile
  final case class File(path: Path) extends TKFile
}

final case class Root(map: TRef[Map[Path, TKFile]]) {

  def exists(path: Path): ZIO[Any, Nothing, Boolean] =
    STM.atomically {
      for {
        files <- map.get
        r     <- ZSTM.succeed(files.contains(path))
      } yield r
    }

  private def getChildren(path: Path): ZSTM[Any, Nothing, Chunk[TKFile]] =
    for {
      files   <- map.get
      children = Chunk.fromIterable(files.filter(_._1.startsWith(path))).filterNot(_._1 == path).map(_._2)
    } yield children

  def tempPath: ZIO[Any, Nothing, Path] =
    STM.atomically {
      for {
        tempPath <- STM.succeed(Paths.get(UUID.randomUUID().toString))
        _        <- map.update(m => m.updated(tempPath, TKFile.File(tempPath)))
      } yield tempPath
    }

  def tempPathIn(dir: Path): ZIO[Any, Nothing, Path] =
    STM.atomically {
      for {
        tempPath <- STM.succeed(Paths.get(dir.toString, UUID.randomUUID().toString))
        _        <- map.update(m => m.updated(tempPath, TKFile.File(tempPath)))
      } yield tempPath
    }

  def tempDirPath: ZIO[Any, Nothing, Path] =
    STM.atomically {
      for {
        tempPath <- ZSTM.succeed(Paths.get(UUID.randomUUID().toString))
        _        <- map.update(m => m.updated(tempPath, TKFile.Dir(tempPath)))
      } yield tempPath
    }

  def list(path: Path): ZIO[Any, IOException, Chunk[Path]] =
    STM.atomically {
      for {
        fileExists <- findFile(path).map(_.isDefined)
        _          <- ZSTM.when(!fileExists)(ZSTM.fail(new FileNotFoundException(s"$path")))
        children   <- getChildren(path).map(_.map(_.path))
      } yield children
    }

  def findFile(path: Path): ZSTM[Any, Nothing, Option[TKFile]] =
    map.get.map(_.get(path))

  def delete(path: Path): ZIO[Any, IOException, Unit] =
    STM.atomically {
      for {
        file <- findFile(path)
        _ <- file match {
               case Some(value) =>
                 value match {
                   case TKFile.Dir(p) =>
                     for {
                       children <- map.get.map(_.get(p))
                       _ <- if (children.isEmpty) {
                              map.update(m => m.removed(p))
                            } else ZSTM.fail(new DirectoryNotEmptyException(s"$p"))
                     } yield ZSTM.unit
                   case TKFile.File(p) => map.update(m => m.removed(p))
                 }
               case None => ZSTM.unit
             }
      } yield ()
    }

}
