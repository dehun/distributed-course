package channel


trait Channel {
  def send(sender: Channel, msg: Message):Unit
  def receive():Option[SentMessage]
}

