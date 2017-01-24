package storage

trait Storage[T] {
  def put[P <: T](t : P)
  def restart()
  def size:Int
  def get(i:Int):Option[T]
  def asList:List[T] = (0 until size).map(i => get(i).get).toList
}
