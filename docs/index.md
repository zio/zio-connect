---
id: index
title: "Introduction to ZIO Connectors"
sidebar_label: "Introduction"
---

ZIO connectors are Sources, Sinks and Pipelines for channeling data. They are easy to use, and they are designed to be
composable. You can use them to build pipelines that can be used to process data.

Each connector is defined as a separate module and can be used independently or in combination with other connectors.

Connectors
--------------

The following connectors are available. These are submodules and are imported individually:

`zio-connect-couchbase` - Couchbase connector uses the [Couchbase Java Client][couchbase-java-client] under the hood.
See [couchbase-connector-examples][couchbase-connector-examples]

`zio-connect-file` - Filesystem connector. See [file-connector-examples][file-connector-examples]

`zio-connect-s3` - Amazon S3 connector uses [zio-aws-s3][zio-aws] under the hood.
See [s3-connector-examples][s3-connector-examples]

[zio-aws]: https://zio.github.io/zio-aws

[couchbase-connector-examples]: https://github.com/zio/zio-connect/tree/master/examples/couchbase-connector-examples/src/main/scala

[file-connector-examples]: https://github.com/zio/zio-connect/tree/master/examples/file-connector-examples/src/main/scala

[s3-connector-examples]: https://github.com/zio/zio-connect/tree/master/examples/s3-connector-examples/src/main/scala

