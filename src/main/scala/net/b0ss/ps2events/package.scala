package net.b0ss

import org.apache.hadoop.fs.RemoteIterator

package object ps2events {
  implicit class HadoopRemoteIterator2ScalaIterator[T](val underlying: RemoteIterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = underlying.hasNext
    override def next(): T = underlying.next()
  }
}
