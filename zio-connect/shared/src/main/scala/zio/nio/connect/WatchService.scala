package zio.nio.connect

import java.nio.file.{WatchKey, WatchService => JWatchService}
import java.util.concurrent.TimeUnit

case class WatchService() extends JWatchService {

  //Closes this watch service.
  //After a watch service is closed, any further attempt to invoke operations upon it will throw ClosedWatchServiceException.
  //If this watch service is already closed then invoking this method has no effect.
  //Throws:
  //IOException – if an I/O error occurs
  override def close(): Unit = ???

  //Retrieves and removes the next watch key, or null if none are present.
  //Returns:
  //the next watch key, or null
  //Throws:
  //ClosedWatchServiceException – if this watch service is closed
  override def poll(): WatchKey = ???

  //Retrieves and removes the next watch key, waiting if necessary up to the specified wait time if none are yet present.
  //Params:
  //timeout – how to wait before giving up, in units of unit unit – a TimeUnit determining how to interpret the timeout parameter
  //Returns:
  //the next watch key, or null
  //Throws:
  //ClosedWatchServiceException – if this watch service is closed, or it is closed while waiting for the next key
  //InterruptedException – if interrupted while waiting
  override def poll(timeout: Long, unit: TimeUnit): WatchKey = ???

  //Retrieves and removes next watch key, waiting if none are yet present.
  //Returns:
  //the next watch key
  //Throws:
  //ClosedWatchServiceException – if this watch service is closed, or it is closed while waiting for the next key
  //InterruptedException – if interrupted while waiting
  override def take(): WatchKey = ???
}
