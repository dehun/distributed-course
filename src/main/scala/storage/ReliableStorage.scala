package storage

import scala.collection.mutable.ListBuffer


class ReliableStorage[T] extends Storage[T] {
  private var data = ListBuffer[T]()

  override def put[P <: T](t: P): Unit = {
    data += t
  }

  override def restart(): Unit = {}

  override def size(): Int = data.size

  override def get(i: Int): Option[T] = {
    if (i >= 0 && i < size()) {
      Some(data(i))
    } else None
  }

  override def delete(i: Int): Unit = data.remove(i)
}
