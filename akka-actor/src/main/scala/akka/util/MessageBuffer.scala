/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.util

import akka.actor.ActorRef

/**
 * A single producer single consumer mutable message buffer
 */
final class MessageBuffer private (
  private var _head: MessageBuffer.Node,
  private var _tail: MessageBuffer.Node) {
  import MessageBuffer._

  private var _size: Int = if (_head eq null) 0 else 1

  def isEmpty: Boolean = _head eq null

  def nonEmpty: Boolean = !isEmpty

  def size: Int = _size

  def append(message: Any, recipient: ActorRef): MessageBuffer = {
    val node: Node = new Node(null, message, recipient)
    if (isEmpty) {
      _head = node
      _tail = node
    } else {
      _tail.next = node
      _tail = node
    }
    _size += 1
    this
  }

  def dropHead(): (Any, ActorRef) = {
    if (nonEmpty) {
      val node = _head
      _head = _head.next
      _size -= 1
      if (isEmpty)
        _tail = null
      (node.message, node.sender)
    } else {
      (null, null)
    }
  }

  def foreach(f: (Any, ActorRef) ⇒ Unit): Unit = {
    var node = _head
    while (node ne null) {
      node(f)
      node = node.next
    }
  }
}

object MessageBuffer {
  private final class Node(var next: Node, val message: Any, val sender: ActorRef) {
    def apply(f: (Any, ActorRef) ⇒ Unit): Unit = {
      f(message, sender)
    }
  }

  def empty: MessageBuffer = new MessageBuffer(null, null)
}

/**
 * A single producer single consumer mutable message buffer map
 *
 * @tparam I (Id type)
 */
final class MessageBufferMap[I] {
  import scala.collection.mutable

  val bufferMap = mutable.Map.empty[I, MessageBuffer]

  def size: Int = {
    var s: Int = 0
    val values = bufferMap.valuesIterator
    while (values.nonEmpty) {
      s += values.next().size
    }
    s
  }

  def add(id: I): Unit = {
    bufferMap.update(id, getOrEmpty(id))
  }

  def append(id: I, message: Any, sender: ActorRef): Unit = {
    bufferMap.update(id, getOrEmpty(id).append(message, sender))
  }

  def remove(id: I): Unit = {
    bufferMap -= id
  }

  def contains(id: I): Boolean = {
    bufferMap.contains(id)
  }

  def getOrEmpty(id: I): MessageBuffer = {
    bufferMap.getOrElse(id, MessageBuffer.empty)
  }
}
