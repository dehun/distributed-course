package storage

import org.scalatest.{FlatSpec, Matchers}
import storage.ReliableStorage

class ReliableStorageSpec extends FlatSpec with Matchers {
  "reliable storage" should "store single value" in {
    val storage = new ReliableStorage[Int]()
    storage.put(10)
    assert (storage.asList === List(10))
  }

  "reliable storage" should "be observable as list" in {
    val storage = new ReliableStorage[Int]()
    (1 to 10).foreach(storage.put)
    assert (storage.asList === (1 to 10).toList)
  }

  it should "store single value and survive restart" in {
    val storage = new ReliableStorage[Int]()
    storage.put(10)
    storage.restart()
    assert (storage.asList === List(10))
  }

  it should "get value by index" in {
    val storage = new ReliableStorage[Int]()
    (1 to 10).foreach(storage.put)
    assert (List(1, 5, 9).map(storage.get).map(_.get) === List(2, 6, 10))
  }

  it should "get value by index be the same as asList" in {
    val storage = new ReliableStorage[Int]()
    (1 to 10).foreach(storage.put)
    assert ((0 until storage.size).map(storage.get).map(_.get) === storage.asList)
  }

  it should "delete value by index" in {
    val storage = new ReliableStorage[Int]()
    (1 to 10).foreach(storage.put)
    List(1, 1, 1, 1, 1).foreach(storage.delete)
    assert (storage.asList === List(1,7,8,9,10))
  }

  it should "store covariant type" in {
    class A(a:Int)
    class B(a:Int, b:Int) extends A(a)

    val storage = new ReliableStorage[A]()
    storage.put(new A(6))
    storage.put(new B(10, 20))

    assert (storage.get(0).get.isInstanceOf[A])
    assert (storage.get(1).get.isInstanceOf[B])
  }

  it should "shrink right to size" in {
    val storage = new ReliableStorage[Int]()
    (1 to 10).foreach(storage.put)
    storage.shrinkRight(5)
    assert (storage.asList === (1 to 5).toList)
  }
}
