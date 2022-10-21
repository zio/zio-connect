package zio.connect.file

import zio.ZIO.attemptBlocking
import zio.stream.{Sink, ZSink, ZStream}
import zio.{Duration, Queue, Ref, Schedule, Scope, Trace, ZIO, ZLayer}

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file._
import java.util.UUID
import scala.jdk.CollectionConverters._

case class LiveFileConnector() extends FileConnector {

  lazy val BUFFER_SIZE = 4096

  lazy val EVENT_NAME = StandardWatchEventKinds.ENTRY_MODIFY.name

  override def deletePath(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(file => ZIO.attemptBlocking(Files.deleteIfExists(file)).refineToOrDie[IOException])

  override def deletePathRecursively(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach { path =>
      val file = path.toFile
      if (file.isDirectory) {
        (listPath(path) >>> deletePathRecursively) *> (ZStream.succeed(path) >>> deletePath)
      } else {
        ZStream.succeed(path) >>> deletePath
      }
    }

  def existsPath(implicit
    trace: Trace
  ): ZSink[Any, IOException, Path, Path, Boolean] =
    ZSink
      .take[Path](1)
      .map(_.headOption)
      .mapZIO {
        case Some(p) => ZIO.attempt(Files.exists(p)).refineToOrDie[IOException]
        case None    => ZIO.succeed(false)
      }

  private def initialRead(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    (for {
      fileSize <- ZIO.attemptBlocking(Files.size(file))
      cursor    = if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
      _        <- ref.update(_ + cursor)
      data     <- attemptBlocking(readBytes(file, cursor))
      _        <- ZIO.foreach(data)(d => queue.offerAll(d))
      _        <- ZIO.foreach(data)(d => ref.update(_ + d.size))
    } yield ()).refineToOrDie[IOException]

  override def listPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.fromJavaStreamZIO(ZIO.attemptBlocking(Files.list(path))).refineToOrDie[IOException]

  override def movePathZIO(
    locator: Path => ZIO[Any, IOException, Path]
  )(implicit trace: Trace): ZSink[Any, IOException, Path, Nothing, Unit] =
    ZSink.foreach(path =>
      (for {
        target <- locator(path)
        _      <- ZIO.attemptBlocking(Files.move(path, target))
      } yield ()).refineToOrDie[IOException]
    )
  private def pollUpdates(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
    for {
      cursor   <- ref.get
      fileSize <- ZIO.attemptBlocking(Files.size(file)).refineToOrDie[IOException]
      data <- attemptBlocking {
                if (fileSize > cursor) {
                  val channel        = FileChannel.open(file.toFile.toPath, Seq(StandardOpenOption.READ): _*)
                  val dataSize: Long = channel.size - cursor
                  val bufSize        = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
                  val buffer         = ByteBuffer.allocate(bufSize)
                  val numBytesRead   = channel.read(buffer, cursor)
                  channel.close
                  if (numBytesRead > 0) Some(buffer.array()) else None
                } else None
              }.refineOrDie { case e: IOException => e }
      _ <- ZIO.foreach(data)(d => queue.offerAll(d))
      _ <- ZIO.foreach(data)(d => ref.update(_ => cursor + d.size))
    } yield ()

  override def readPath(path: => Path)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.fromPath(path).refineOrDie { case e: IOException => e }

  private def readBytes(file: Path, cursor: Long): Option[Array[Byte]] = {
    val channel        = FileChannel.open(file.toFile.toPath, Seq(StandardOpenOption.READ): _*)
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

  private def readData(
    file: Path,
    watchService: WatchService,
    cursor: Long
  ): ZIO[Any, IOException, Option[Array[Byte]]] =
    ZIO
      .attemptBlocking(Option(watchService.poll))
      .flatMap {
        case Some(key) =>
          for {
            events <- ZIO.attemptBlocking(key.pollEvents)
            _      <- ZIO.attemptBlocking(key.reset)
            r <-
              if (events.asScala.exists(r => r.kind.name == EVENT_NAME && file.toString.endsWith(r.context.toString)))
                ZIO.attempt(readBytes(file, cursor))
              else ZIO.none
          } yield r
        case _ => ZIO.none
      }
      .refineOrDie { case e: IOException => e }

  private def registerWatchService(file: Path): ZIO[Any, IOException, WatchService] =
    (for {
      watchService <- ZIO.attemptBlocking(file.getFileSystem.newWatchService())
      _ <- ZIO
             .attemptBlocking(file.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY))
    } yield watchService).refineToOrDie[IOException]

  override def tailPath(path: => Path, freq: => Duration)(implicit trace: Trace): ZStream[Any, IOException, Byte] =
    ZStream.unwrap(for {
      fileNotFound <- ZIO.attemptBlocking(Files.notExists(path)).refineToOrDie[IOException]
      _            <- ZIO.fail(new FileNotFoundException(s"$path")).when(fileNotFound)
      queue        <- Queue.bounded[Byte](BUFFER_SIZE)
      fileSize     <- ZIO.attemptBlocking(Files.size(path)).refineToOrDie[IOException]
      cursor        = if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
      ref          <- Ref.make(cursor)
      _            <- pollUpdates(path, queue, ref).repeat[Any, Long](Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue))

  override def tailPathUsingWatchService(path: => Path, freq: => Duration)(implicit
    trace: Trace
  ): ZStream[Any, IOException, Byte] =
    ZStream.unwrapScoped((for {
      fileNotFound <- ZIO.attemptBlocking(Files.notExists(path))
      _            <- ZIO.fail(new FileNotFoundException(s"$path")).when(fileNotFound)
      queue        <- Queue.bounded[Byte](BUFFER_SIZE)
      ref          <- Ref.make(0L)
      _            <- initialRead(path, queue, ref)
      parent <- ZIO
                  .attemptBlocking(Option(path.getParent))
                  .flatMap(ZIO.fromOption(_).orElseFail(new IOException(s"Parent directory not found for $path")))
      watchService <- registerWatchService(parent)
      _            <- watchUpdates(path, watchService, queue, ref).repeat[Any, Long](Schedule.fixed(freq)).forever.fork
    } yield ZStream.fromQueueWithShutdown(queue)).refineToOrDie[IOException])

  override def tempPath(implicit trace: Trace): ZStream[Any, IOException, Path] = {
    val scopedTempFile: ZIO[Scope, IOException, Path] =
      ZIO.acquireRelease(
        ZIO
          .attemptBlocking(
            Files.createTempFile(UUID.randomUUID().toString, ".tmp")
          )
          .orDie
      )(path => (ZStream(path) >>> deletePath).orDie)

    ZStream.unwrapScoped(
      scopedTempFile.map(path =>
        ZStream
          .fromZIO(ZIO.succeed(path))
      )
    )
  }

  override def tempDirPath(implicit trace: Trace): ZStream[Any, IOException, Path] = {

    val scopedTempDir: ZIO[Scope, IOException, Path] =
      ZIO.acquireRelease(
        ZIO
          .attemptBlocking(
            Files.createTempDirectory(UUID.randomUUID().toString)
          )
          .orDie
      )(path => (ZStream(path) >>> deletePathRecursively).orDie)

    ZStream.unwrapScoped(
      scopedTempDir.map(path =>
        ZStream
          .fromZIO(ZIO.succeed(path))
      )
    )
  }

  override def tempPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(
              Files.createTempFile(dirPath, UUID.randomUUID().toString, ".tmp")
            )
            .orDie
        )(path => (ZStream(path) >>> deletePath).orDie)
        .map(a => ZStream(a))
    )

  override def tempDirPathIn(dirPath: => Path)(implicit trace: Trace): ZStream[Any, IOException, Path] =
    ZStream.unwrapScoped(
      ZIO
        .acquireRelease(
          ZIO
            .attemptBlocking(
              Files.createTempDirectory(dirPath, UUID.randomUUID().toString)
            )
            .orDie
        )(path => (ZStream(path) >>> deletePathRecursively).orDie)
        .map(path =>
          ZStream
            .fromZIO(ZIO.succeed(path))
        )
    )

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

  override def writePath(path: => Path)(implicit trace: Trace): Sink[IOException, Byte, Nothing, Unit] =
    ZSink
      .fromPath(path)
      .refineOrDie { case e: IOException => e }
      .as(())
      .ignoreLeftover

}

object LiveFileConnector {
  val layer: ZLayer[Any, Nothing, FileConnector] = ZLayer.succeed(LiveFileConnector())

}
