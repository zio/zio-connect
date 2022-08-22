package zio.connect.file

import zio.{Duration, Queue, Ref, Schedule, Scope, Trace, ZIO, ZLayer}
import zio.ZIO.{attemptBlocking, whenZIO}
import zio.nio.file.WatchService
import zio.stream.{Sink, ZSink, ZStream}
import zio.nio.connect.Files

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Path, StandardOpenOption, StandardWatchEventKinds}

case class LiveFileConnector(files: Files, watchService: WatchService) extends FileConnector {

  lazy val BUFFER_SIZE = 4096

  lazy val EVENT_NAME = StandardWatchEventKinds.ENTRY_MODIFY.name

  def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    files.list(dir).refineOrDie { case e: IOException => e }

  def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.fromPath(file).refineOrDie { case e: IOException => e }

  def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(for {
      _     <- whenZIO(files.notExists(file))(ZIO.fail(new FileNotFoundException(file.toString)))
      queue <- Queue.bounded[Byte](BUFFER_SIZE)
      cursor <- ZIO.attempt {
                  val fileSize = java.nio.file.Files.size(file)
                  if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
                }.refineOrDie { case e: IOException => e }
      ref <- Ref.make(cursor)
      _   <- pollUpdates(file, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue))

  private def pollUpdates(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    for {
      cursor   <- ref.get
      fileSize <- files.size(file)
      data <- attemptBlocking {
                if (fileSize > cursor) {
                  val channel        = FileChannel.open(file, Seq(StandardOpenOption.READ): _*)
                  val dataSize: Long = channel.size - cursor
                  val bufSize        = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
                  val buffer         = ByteBuffer.allocate(bufSize)
                  val numBytesRead   = channel.read(buffer, cursor)
                  channel.close
                  if (numBytesRead > 0) Some(buffer.array()) else None
                } else None
              }.tapError(e => ZIO.debug(e.toString)).refineOrDie { case e: IOException => e }
      _ <- ZIO.foreach(data)(d => queue.offerAll(d))
      _ <- ZIO.foreach(data)(d => ref.update(_ => cursor + d.size))
    } yield ()

  def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    ZStream.unwrapScoped(for {
      _     <- whenZIO(files.notExists(file))(ZIO.fail(new FileNotFoundException(file.toString)))
      queue <- Queue.bounded[Byte](BUFFER_SIZE)
      ref   <- Ref.make(0L)
      _     <- initialRead(file, queue, ref)
      watchService <-
        ZIO
          .fromOption(Option(file.getParent))
          .flatMap(a => registerWatchService(a))
          .refineOrDieWith { case e: IOException => e }(er => new RuntimeException(er.toString))
      _ <- watchUpdates(file, watchService, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue))

  private def initialRead(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    (for {
      cursor <- ZIO.attempt {
                  val fileSize = java.nio.file.Files.size(file)
                  if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
                }
      _    <- ref.update(_ + cursor)
      data <- attemptBlocking(readBytes(file, cursor))
      _    <- ZIO.foreach(data)(d => queue.offerAll(d))
      _    <- ZIO.foreach(data)(d => ref.update(_ + d.size))
    } yield ()).refineOrDie { case e: IOException => e }

  private def registerWatchService(file: Path): ZIO[Scope, IOException, WatchService] =
    (for {
      _ <- zio.nio.file.Path.fromJava(file).register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
    } yield watchService).refineOrDie { case e: IOException => e }

  private def watchUpdates(
    file: Path,
    watchService: WatchService,
    queue: Queue[Byte],
    ref: Ref[Long]
  ): ZIO[Any, IOException, Unit] =
    for {
      cursor <- ref.get
      data   <- readData(file, watchService, cursor)
      _      <- ZIO.foreach(data)(d => queue.offerAll(d))
      _      <- ZIO.foreach(data)(d => ref.update(_ + d.size))
    } yield ()

  private def readData(
    file: Path,
    watchService: WatchService,
    cursor: Long
  ): ZIO[Any, IOException, Option[Array[Byte]]] =
    watchService.poll.flatMap {
      case Some(key) =>
        for {
          events <- key.pollEvents
          _      <- key.reset
          r <- if (events.exists(r => r.kind.name == EVENT_NAME && file.toString().endsWith(r.context.toString)))
                 ZIO.attempt(readBytes(file, cursor))
               else ZIO.none
        } yield r
      case _ => ZIO.none
    }.refineOrDie { case e: IOException => e }

  private def readBytes(file: Path, cursor: Long): Option[Array[Byte]] = {
    val channel        = FileChannel.open(file, Seq(StandardOpenOption.READ): _*)
    val dataSize: Long = channel.size - cursor
    if (dataSize > 0) {
      val bufSize      = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
      val buffer       = ByteBuffer.allocate(bufSize)
      val numBytesRead = channel.read(buffer, cursor)
      channel.close
      if (numBytesRead > 0) Some(buffer.array()) else None
    } else {
      channel.close
      None
    }
  }

  override def writeFile(file: => Path)(implicit trace: Trace): Sink[IOException, Byte, Nothing, Unit] =
    ZSink
      .fromPath(file)
      .refineOrDie { case e: IOException => e }
      .as(())
      .ignoreLeftover

  override def deleteFile(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(file => files.delete(file))

  override def moveFile(locator: Path => Path)(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(file => files.move(file, locator(file)))

}

object LiveFileConnector {
  def layer: ZLayer[Files with WatchService, Nothing, FileConnector] =
    ZLayer.fromZIO {
      for {
        watchService <- ZIO.service[WatchService]
        files        <- ZIO.service[Files]
      } yield new LiveFileConnector(files, watchService)
    }
}
