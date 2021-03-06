package zio.connect

import zio._
import zio.stream._
import zio.blocking._
import zio.clock.Clock
import zio.duration._

package object file {
  import java.io.IOException
  import java.io.RandomAccessFile
  import java.nio.ByteBuffer
  import zio.nio.file.Files
  import java.nio.file.Path  // Should we use zio.nio.core.file.Path instead?
  import java.nio.file.{StandardWatchEventKinds, WatchService}
  import scala.jdk.CollectionConverters._

  type FileConnector = Has[FileConnector.Service]
  
  object FileConnector {
    trait Service {
      def listDir(dir: Path): ZStream[Blocking, IOException, Path]

      def readFile(file: Path): ZStream[Blocking, IOException, Byte]
      
      def tailFile(file: Path, freq: Duration): ZStream[Blocking with Clock, IOException, Byte]

      def tailFileUsingWatchService(file: Path, freq: Duration): ZStream[Blocking with Clock, IOException, Byte]
      // def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit]

    }

     object Service {
       val live: Service = new Service {

         def listDir(dir: Path): ZStream[Blocking, IOException, Path] = {
           ZStream.fromEffect(effectBlocking(java.nio.file.Files.list(dir).iterator()))
             .flatMap(Files.fromJavaIterator)
             .refineOrDie { case e: IOException => e }
         }

         def readFile(file: Path): ZStream[Blocking, IOException, Byte] =
           ZStream.fromEffect(Files.readAllBytes(zio.nio.core.file.Path.fromJava(file)))
             .flatMap(r => ZStream.fromChunk[Byte](r))

         //         def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit] = ???

         def tailFile(file: Path, freq: Duration): ZStream[Blocking with Clock, IOException, Byte] = {
           ZStream.unwrap(for {
             queue  <- Queue.bounded[Byte](BUFFER_SIZE)
             cursor <- ZIO.effect {
                         val fileSize = file.toFile.length
                         if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
                       }.refineOrDie { case e: IOException => e }
             ref    <- Ref.make(cursor)
             _      <- pollUpdates(file, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
           } yield ZStream.fromQueueWithShutdown[Blocking with Clock, IOException, Byte](queue))
         }

         lazy val BUFFER_SIZE = 4096

         def pollUpdates(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Blocking, IOException, Unit] =
           for {
             cursor <- ref.get
             data   <- effectBlocking {
                         if (file.toFile.length > cursor) {
                           val reader = new RandomAccessFile(file.toFile, "r")
                           val channel = reader.getChannel
                           val dataSize: Long = channel.size - cursor
                           val bufSize = if (dataSize > BUFFER_SIZE) BUFFER_SIZE else dataSize.toInt
                           val buffer = ByteBuffer.allocate(bufSize)
                           val numBytesRead = channel.read(buffer, cursor)
                           reader.close
                           if (numBytesRead > 0) Some(buffer.array()) else None
                         } else None
                       }.refineOrDie { case e: IOException => e }
             _      <- ZIO.foreach(data)(d => queue.offerAll(d.toIterable))
             _      <- ZIO.foreach(data)(d => ref.update(_ => cursor + d.size))
           } yield ()


         def tailFileUsingWatchService(file: Path, freq: Duration): ZStream[Blocking with Clock, IOException, Byte] = {
           ZStream.unwrap(for {
             queue <- Queue.bounded[Byte](BUFFER_SIZE)
             ref <- Ref.make(0L)
             _ <- initialRead(file, queue, ref)
             watchService <- registerWatchService(file.getParent)
             _ <- watchUpdates(file, watchService, queue, ref).repeat(Schedule.fixed(freq)).forever.fork
           } yield ZStream.fromQueueWithShutdown[Blocking with Clock, IOException, Byte](queue))
         }

         lazy val EVENT_NAME  = StandardWatchEventKinds.ENTRY_MODIFY.name

         def initialRead(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Blocking, IOException, Unit] = {
           (for {
             cursor <- ZIO.effect {
               val fileSize = file.toFile.length
               if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
             }
             _      <- ref.update(_ + cursor)
             data   <- effectBlocking(readBytes(file, cursor))
             _      <- ZIO.foreach(data)(d => queue.offerAll(d.toIterable))
             _      <- ZIO.foreach(data)(d => ref.update(_ + d.size))
           } yield ()).refineOrDie { case e: IOException => e }
         }

         def registerWatchService(file: Path): ZIO[Any, IOException, WatchService] =
           ZIO.effect {
             val watchService = file.getFileSystem.newWatchService
             file.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)
             watchService
           }.refineOrDie { case e: IOException => e }

         def watchUpdates(file: Path, watchService: WatchService, queue: Queue[Byte], ref: Ref[Long]): ZIO[Blocking, IOException, Unit] =
           for {
             cursor <- ref.get
             data   <- readData(file, watchService, cursor)
             _      <- ZIO.foreach(data)(d => queue.offerAll(d.toIterable))
             _      <- ZIO.foreach(data)(d => ref.update(_ + d.size))
           } yield ()

         def readData(file: Path, watchService: WatchService, cursor: Long): ZIO[Blocking, IOException, Option[Array[Byte]]] =
           effectBlocking {
             Option(watchService.poll) match {
               case Some(key) => {
                 val events = key.pollEvents.asScala
                 key.reset
                 if (events.exists(r => r.kind.name == EVENT_NAME && r.context.asInstanceOf[Path].endsWith(file.getFileName)))
                   readBytes(file, cursor)
                 else None
               }
               case _ => None
             }
           }.refineOrDie { case e: IOException => e }

         def readBytes(file: Path, cursor: Long): Option[Array[Byte]] = {
           val reader  = new RandomAccessFile(file.toFile, "r")
           val channel = reader.getChannel
           val dataSize: Long = channel.size - cursor
           if (dataSize > 0) {
             val bufSize = if (dataSize > BUFFER_SIZE ) BUFFER_SIZE else dataSize.toInt
             val buffer  = ByteBuffer.allocate(bufSize)
             val numBytesRead = channel.read(buffer, cursor)
             reader.close
             if (numBytesRead > 0) Some(buffer.array()) else None
           } else {
             reader.close
             None
           }
         }

       }

     }

    val any: ZLayer[FileConnector, Nothing, FileConnector] = ZLayer.requires[FileConnector]
      // Do we need this?

     val live: Layer[Nothing, FileConnector] = ZLayer.succeed(Service.live)
  }

  def listDir(dir: Path): ZStream[FileConnector with Blocking, IOException, Path] =
    ZStream.accessStream(_.get.listDir(dir))

  def readFile(file: Path): ZStream[FileConnector with Blocking, IOException, Byte] =
    ZStream.accessStream(_.get.readFile(file))

  def tailFile(file: Path, freq: Duration): ZStream[FileConnector with Blocking with Clock, IOException, Byte] =
    ZStream.accessStream(_.get.tailFile(file, freq))

  // def writeFile(file: Path): ZSink[FileConnector, IOException, Chunk[Byte], Unit] = 
    // ZIO.accessM[FileConnector](_.get.writeFile(file))

  // def delete: ZSink[FileConnector, IOException, Path, Unit] = ???  // Should it be Stream or Sink?

  // def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???
}

