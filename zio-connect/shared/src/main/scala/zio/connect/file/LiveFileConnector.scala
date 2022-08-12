package zio.connect.file

import zio.{Cause, Duration, Queue, Ref, Schedule, Trace, ZIO, ZLayer}
import zio.ZIO.attemptBlocking
import zio.nio.file.Path
import zio.stream.{Sink, ZChannel, ZSink, ZStream}

import java.io.{ IOException, RandomAccessFile }
import java.nio.ByteBuffer
import java.nio.file.{StandardWatchEventKinds, WatchService}
import scala.jdk.CollectionConverters._

case class LiveFileConnector() extends FileConnector {

  lazy val BUFFER_SIZE = 4096

  lazy val EVENT_NAME = StandardWatchEventKinds.ENTRY_MODIFY.name

  def listDir(dir: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    zio.nio.file.Files.list(dir).refineOrDie { case e: IOException => e }

  def readFile(file: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.fromPath(file.toFile.toPath).refineOrDie { case e: IOException => e }

  def tailFile(file: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(for {
      queue <- Queue.bounded[Byte](BUFFER_SIZE)
      cursor <- ZIO.attempt {
                  val fileSize = file.toFile.length
                  if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
                }.refineOrDie { case e: IOException => e }
      ref <- Ref.make(cursor)
      _   <- pollUpdates(file, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue))

  private def pollUpdates(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    for {
      cursor <- ref.get
      data <- attemptBlocking {
                if (file.toFile.length > cursor) {
                  val reader         = new RandomAccessFile(file.toFile, "r")
                  val channel        = reader.getChannel
                  val dataSize: Long = channel.size - cursor
                  val bufSize        = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
                  val buffer         = ByteBuffer.allocate(bufSize)
                  val numBytesRead   = channel.read(buffer, cursor)
                  reader.close
                  if (numBytesRead > 0) Some(buffer.array()) else None
                } else None
              }.refineOrDie { case e: IOException => e }
      _ <- ZIO.foreach(data)(d => queue.offerAll(d))
      _ <- ZIO.foreach(data)(d => ref.update(_ => cursor + d.size))
    } yield ()

  def tailFileUsingWatchService(file: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(for {
      queue        <- Queue.bounded[Byte](BUFFER_SIZE)
      ref          <- Ref.make(0L)
      _            <- initialRead(file, queue, ref)
      watchService <- registerWatchService(Path.fromJava(file.toFile.toPath.getParent))
      _            <- watchUpdates(file, watchService, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue))

  private def initialRead(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    (for {
      cursor <- ZIO.attempt {
                  val fileSize = file.toFile.length
                  if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
                }
      _    <- ref.update(_ + cursor)
      data <- attemptBlocking(readBytes(file, cursor))
      _    <- ZIO.foreach(data)(d => queue.offerAll(d))
      _    <- ZIO.foreach(data)(d => ref.update(_ + d.size))
    } yield ()).refineOrDie { case e: IOException => e }

  private def registerWatchService(file: Path): ZIO[Any, IOException, WatchService] =
    ZIO.attempt {
      val watchService = file.toFile.toPath.getFileSystem.newWatchService
      file.toFile.toPath.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
      watchService
    }.refineOrDie { case e: IOException => e }

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
    attemptBlocking {
      Option(watchService.poll) match {
        case Some(key) => {
          val events = key.pollEvents.asScala
          key.reset
          if (
            events.exists(r =>
              r.kind.name == EVENT_NAME && r.context
                .asInstanceOf[Path]
                .endsWith(Path.fromJava(file.toFile.toPath.getFileName))
            )
          )
            readBytes(file, cursor)
          else None
        }
        case _ => None
      }
    }.refineOrDie { case e: IOException => e }

  private def readBytes(file: Path, cursor: Long): Option[Array[Byte]] = {
    val reader         = new RandomAccessFile(file.toFile, "r")
    val channel        = reader.getChannel
    val dataSize: Long = channel.size - cursor
    if (dataSize > 0) {
      val bufSize      = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
      val buffer       = ByteBuffer.allocate(bufSize)
      val numBytesRead = channel.read(buffer, cursor)
      reader.close
      if (numBytesRead > 0) Some(buffer.array()) else None
    } else {
      reader.close
      None
    }
  }

  override def writeFile(file: => Path)(implicit trace: Trace): Sink[IOException, Byte, Nothing, Unit] =
    ZSink.fromChannel(
      ZSink
        .fromPath(file.toFile.toPath)
        .map(_ => ())
        .ignoreLeftover
        .channel
        .catchAll {
          case e: IOException => ZChannel.fail(e)
          case e              => ZChannel.failCause(Cause.die(e))
        }
    )
}

object LiveFileConnector {
  def layer: ZLayer[Any, Nothing, FileConnector] =
    ZLayer.succeed(new LiveFileConnector)
}
