---
id: quickstart_index
title: "Quick Start"
---

Setup
-----

```
//support scala 2.12 / 2.13 / 3.2

libraryDependencies += "dev.zio" %% "zio-connect" % "<version>"
```

How to use it ?
---------------
Todo

Test / Stub
-----------
A stub implementation of FileConnector is provided for testing purposes via the `TestFileConnector.layer`. It uses
internally an in memory filesystem to avoid the actual creation of files.

```scala
object MyTestSpec {

  override def spec =
    suite("TestFileConnectorSpec")(...)
      .provideSome[Scope](TestFileConnector.layer)

}
```

Examples
--------
Todo