package storage


class ReliableStorage[T] extends Storage[T] {
  private var data:List[T] = List[T]()

  override def put[P <: T](t: P): Unit = {
    data = data ::: List(t)
  }

  override def restart(): Unit = {}

  override def size(): Int = data.size

  override def get(i: Int): Option[T] = {
    if (i >= 0 && i < size()) {
      Some(data(i))
    } else None
  }
}
