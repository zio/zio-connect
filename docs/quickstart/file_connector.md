---
id: quickstart_file_connector
title: "FileConnector"
---

Setup
-----

```
//support scala 2.12 / 2.13 / 3.2

libraryDependencies += "dev.zio" %% "zio-connect" % "<version>"
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
  fileExists <- existsPath(file).tap(a => ZIO.debug(s"$path exists? $a"))
  _          <- stream >>> writePath(file)
  _          <- readPath(file) >>> sink
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
      .provide(zio.connect.file.test)

}
```

Examples
--------
Todo