---
id: quickstart_index
title: "Quick Start"
---

Connectors are easy to use, and they are designed to be composable. You can use them to build pipelines that can be used to process data.
Each connector is defined as a separate module and can be used independently or in combination with other connectors.

Connectors
--------------

The following connectors are available:

`zio-connect-file` - Filesystem connector. [file-connector-examples][file-connector-examples]

`zio-connect-s3` - Amazon S3 connector uses [zio-aws-s3][zio-aws] under the hood. [s3-connector-examples][s3-connector-examples]

[zio-aws]: https://zio.github.io/zio-aws
[file-connector-examples]: https://github.com/zio/zio-connect/tree/master/examples/file-connector-examples/src/main/scala
[s3-connector-examples]: https://github.com/zio/zio-connect/tree/master/examples/s3-connector-examples/src/main/scala

