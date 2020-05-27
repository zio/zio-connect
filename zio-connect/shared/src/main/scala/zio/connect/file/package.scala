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
      def listDir(dir: Path): Stream[IOException, Path]

      def readFile(file: Path): Stream[IOException, Byte]
      
      def tailFile(file: Path): Stream[IOException, Byte]

      // def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit]

    }

     object Service {
       val live: Service = new Service {

         def listDir(dir: Path): Stream[IOException, Path] = {
             ZStream.fromEffect(effectBlocking(java.nio.file.Files.list(dir).iterator()))
               .flatMap(Files.fromJavaIterator)
               .provideLayer(Blocking.live)
               .refineOrDie{ case e: IOException => e }
         }

         def readFile(file: Path): Stream[IOException, Byte] =
           ZStream.fromEffect(Files.readAllBytes(zio.nio.core.file.Path.fromJava(file)).provideLayer(Blocking.live))
             .flatMap(r => ZStream.fromChunk[Byte](r))

//         def writeFile(file: Path): Sink[IOException, Chunk[Byte], Unit] = ???

         def tailFile(file: Path): Stream[IOException, Byte] = {
           ZStream.unwrap(for {
             queue        <- Queue.bounded[Byte](BUFFER_SIZE)
             ref          <- Ref.make(0L)
             _            <- initialRead(file, queue, ref)
             watchService <- registerWatchService(file.getParent)
             _            <- pollUpdates(file, watchService, queue, ref).repeat(Schedule.fixed(1000.milliseconds)).forever.fork
           } yield ZStream.fromQueueWithShutdown[Clock, IOException, Byte](queue)).provideLayer(Clock.live)
         }

         lazy val BUFFER_SIZE = 8192
         lazy val EVENT_NAME  = StandardWatchEventKinds.ENTRY_MODIFY.name

         def initialRead(file: Path, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] = {
           (for {
             cursor <- ZIO.effect {
               val fileSize = file.toFile.length
               if (fileSize > BUFFER_SIZE) fileSize - BUFFER_SIZE else 0L
             }
             _      <- ref.update(_ + cursor)
             data   <- ZIO.effect(readBytes(file, cursor))
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

         def pollUpdates(file: Path, watchService: WatchService, queue: Queue[Byte], ref: Ref[Long]): ZIO[Any, IOException, Unit] =
           for {
             cursor <- ref.get
             data   <- readData(file, watchService, cursor)
             _      <- ZIO.foreach(data)(d => queue.offerAll(d.toIterable))
             _      <- ZIO.foreach(data)(d => ref.update(_ + d.size))
           } yield ()

         def readData(file: Path, watchService: WatchService, cursor: Long): ZIO[Any, IOException, Option[Array[Byte]]] =
           ZIO.effect {
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

  def listDir(dir: Path): ZStream[FileConnector, IOException, Path] = 
    ZStream.accessStream[FileConnector](_.get.listDir(dir))

  def readFile(file: Path): ZStream[FileConnector, IOException, Byte] =
    ZStream.accessStream[FileConnector](_.get.readFile(file))

  def tailFile(file: Path): ZStream[FileConnector, IOException, Byte] = 
    ZStream.accessStream[FileConnector](_.get.tailFile(file))

  // def writeFile(file: Path): ZSink[FileConnector, IOException, Chunk[Byte], Unit] = 
    // ZIO.accessM[FileConnector](_.get.writeFile(file))

  // def delete: ZSink[FileConnector, IOException, Path, Unit] = ???  // Should it be Stream or Sink?

  // def move(locator: Path => Path): ZSink[FileConnector, IOException, Path, Unit] = ???
}

