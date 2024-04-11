## Redeus

`Redeus` is a distributed key-value database compatiable with Redis Protocol, implemented with `Rust` using `RocksDB` as storage engine .

## Build and run Redeus

1. Build

```shell
cargo build
```

2. Run

```shell
cargo run rudeus
```

## Developer Guide

1. To speed up unit test process, you can first create a `ramdisk` then specify the unit test working directory by set environment variable `RUDEUS_TESTDIR`

For linux, use the following commands:

```
mkdir /path/to/ramdisk
mount -t tmpfs -o size=128M tmpfs /path/to/ramdisk
export RUDEUS_TESTDIR=/path/to/ramdisk
cargo test
```

Similar thing should work on windows too.