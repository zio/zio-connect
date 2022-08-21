package zio.nio.connect

import zio.{Scope, ZIO, ZLayer}
import zio.nio.file.{FileSystem, WatchService}

import java.io.IOException

object WatchServiceLayers {

  val live: ZLayer[Scope, IOException, WatchService] =
    ZLayer.fromZIO(WatchService.forDefaultFileSystem)

  val inMemory: ZLayer[Scope with FileSystem, IOException, WatchService] =
    ZLayer.fromZIO(ZIO.serviceWithZIO[FileSystem](_.newWatchService))
}
