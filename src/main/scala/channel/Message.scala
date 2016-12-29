package channel


trait Message {
}

case class SentMessage(sender:Channel, msg:Message)
