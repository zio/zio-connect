---
id: file-connector
title: "File Connector"
---

Setup
-----

```scala
libraryDependencies += "dev.zio" %% "zio-connect-file" % "@VERSION@"
```

How to use it ?
---------------
All available FileConnector combinators and operations are available in the package object `zio.connect.file`, you only
need to import `zio.connect.file._`

For instance the code below uses at each step a FileConnector operator.

```scala
import zio.connect.file._

val stream: ZStream[Any, Nothing, Byte] = ???
val sink: ZSink[Any, Nothing, Byte, Nothing, Unit] = ???

for {
  dir        <- tempDirPath
  path       <- tempPathIn(dir)
  fileExists <- ZStream.fromZIO((ZStream(path) >>> existsPath(file)).tap(a => ZIO.debug(s"$path exists? $a")))
  _          <- ZStream.fromZIO(stream >>> writePath(file))
  _          <- ZStream.fromZIO(readPath(file) >>> sink)
} yield fileExists
```

- tempDirPath - creates a directory and returns a ZStream with a single Path value, it will delete it once the for
  comprehension completes
- tempPathIn - creates a file in the given directory and returns a ZStream with a single Path value, it will delete it
  once the for comprehension completes
- existsPath - creates a ZStream with a single value
- writePath - creates ZSink for writing to given file
- readPath - creates a ZStream for reading from given file

Test / Stub
-----------
A stub implementation of FileConnector is provided for testing purposes via the `TestFileConnector.layer`. It uses
internally an in memory filesystem to avoid the actual creation of files.

```scala
package zio.connect.file

import zio.Scope

object MyTestSpec extends ZIOSpecDefault{

  override def spec =
    suite("MyTestSpec")(???)
      .provide(zio.connect.file.fileConnectorTestLayer)

}
```

Operators & Examples
---------

readX
---

- readFile | readFileName | readPath | readURI

Creates a ZStream for reading a file's content from a path/file/...
The stream ends once all content is read.

```scala
import zio.connect.file._

def example(path: Path): ZStream[Any, IOException, Byte] =
   readPath(path)
```

writeX
---

- writeFile | writeFileName | writePath | writeURI

Creates a ZSink for writing to a file.
The stream ends once all content is read.

```scala
import zio.connect.file._

def example(path: Path): ZSink[Any, IOException, Byte, Nothing, Unit] =
   writePath(path)
```

tailX
---

- tailFile | tailFileName | tailPath | tailURI

Creates a ZStream for reading a file's content from a path/file/...
The stream never ends emitting; it will keep polling the file (with given frequency) even after all content is read.

```scala
import zio.connect.file._

def example(path: Path, freq: Duration): ZStream[Any, IOException, Byte] =
   tailPath(path, freq)
```

tailXUsingWatchService
---

- tailFileUsingWatchService | tailFileNameUsingWatchService | tailPathUsingWatchService | tailURIUsingWatchService

Creates a ZStream for reading a file's content from a path/file/...
The stream never ends emitting; it will keep polling the file (with given frequency and if watchService detects
changes) even after all content is read.

```scala
import zio.connect.file._

def example(path: Path, freq: Duration): ZStream[Any, IOException, Byte] =
   tailPathUsingWatchService(path, freq)
```

deleteX
---

- deleteFile | deleteFileName | deletePath | deleteURI

It provides a sink that deletes the file or directory.
To delete non empty directories use the deleteRecursivelyX variants.

```scala
import zio.connect.file._

def example(paths: ZStream[Any, Nothing, Path]) =
   paths >>> deletePath
```

deleteXRecursively
---

- deleteFileRecursively | deleteFileNameRecursively | deletePathRecursively | deleteURIRecursively

Same as deleteX operator + it can delete non empty directories.

existsX
---

- existsFile | existsFileName | existsPath | existsURI

Creates a ZSink that emits true if the first path/file/... exists or a false
otherwise. The files received after the first one can be found in the sink's leftovers.

```scala
import zio.connect.file._

def example(stream: ZStream[Any, IOException, Path]): ZIO[Any, IOException, Boolean] = 
    stream >>> existsPath
```

listX
---

- listFile | listFileName | listPath | listURI

Returns the files inside the given path/file/.... Fails if provided path is not a dir.

```scala
import zio.connect.file._

def example(path: Path): ZStream[Any, IOException, Path] = 
    listPath(path)
```

moveX
---

- moveFile | moveFileName | movePath | moveURI

You can provide a function `Path => Path` and you will get a Sink that when given a path p1 will call the function
with p1 and so get a p2, then move the file/dir at p1 to p2.

```scala
import zio.connect.file._

def example(locator: Path => Path): ZSink[Any, IOException, Path, Nothing, Unit] = 
    movePath(locator)
```

moveXZIO
---

- moveFileZIO | moveFileNameZIO | movePathZIO | moveURIZIO

Same as moveX except determining the destination is effectful.

```scala
import zio.connect.file._

def example(locator: Path => ZIO[Any, IOException, Path]): ZSink[Any, IOException, Path, Nothing, Unit] = 
    movePathZIO(locator)
```

tempX / tempXIn / tempDirX / tempDirXIn
---

- tempFile | tempFileName | tempPath | tempURI
- tempFileIn | tempFileNameIn | tempPathIn | tempURIIn
- tempDirFile | tempDirFileName | tempDirPath | tempDirURI
- tempDirFileIn | tempDirFileNameIn | tempDirPathIn | tempDirURIIn

With this set of operators we can create temporary files and directories that will be cleaned up automatically once the
effect using them completes.

The below example creates the following structure:

* file
* baseDir
    * subDir
        * fileInSubDir
    * fileInBaseDir

```scala
import zio.connect.file._

def example() = 
    for {
       file          <- tempPath
       baseDir       <- tempDirPath
       subDir        <- tempDirPathIn(baseDir)
       fileInBaseDir <- tempPathIn(baseDir)
       fileInSubDir  <- tempPathIn(subDir)
    } yield ()
```
