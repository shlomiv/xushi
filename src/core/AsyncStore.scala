package core

import scala.collection.mutable.{HashSet, Set}
import scala.collection.immutable.HashMap

class AsyncStore[A] {
  protected class Node(val key: String, var v: Option[A], var childs: Set[Node], var listeners: Set[Either[(String, Option[A]) => Unit, (List[(String, Option[A], Op)])=>Unit]]);
  
  protected val root = new Node("", None, HashSet(), HashSet())

  // a map containing removal code for a certain key  
  protected var stopMap = new HashMap[Object, () => Unit]
  
  // notify all listeners of node about the value v in fullKey
  protected def notifyChange(fullKey: String, v: Option[A], node: Node) = node.listeners.foreach(_.left.foreach(_.apply(fullKey, v)))

  // find a child named key in node
  protected val finder = (key: String, node: Node) => node.childs.find(_.key.equals(key))
  
  // get the child named key in node or create it as needed
  protected val adder = (key: String, node: Node) => finder(key, node).orElse({
    val n = new Node(key, None, Set(), Set())
    node.childs += n
    Some(n)
  })

  // traverse the prefix tree using a scanner, invoking fn on the leaf node, and pfn on every parent in that tree
  protected def onKeyPath(scanner: (String, Node) => Option[Node])(fn: (Node, String) => Unit, pfn: (Node, String, List[String]) => Unit)(key: List[String], node: Node): Unit = key match {
    case key :: Nil => scanner(key, node).foreach(fn(_, key))
    case key :: rest => {
      scanner(key, node).foreach(n=> {
        onKeyPath(scanner)(fn, pfn)(rest, n)
        pfn(n, key, rest)
      })
    }
    case Nil =>
  }
  
  // find a certain key in a prefix tree rooted at node
  protected def inFindKey(key: List[String], node: Node): Option[Node] = key match {
  case key :: Nil => finder(key, node)
  case key :: rest => finder(key, node).flatMap(inFindKey(rest, _))
  case Nil => None
  }

  // add fullKey to the prefix tree
  protected def inPut(fullKey: String, key: List[String], v: A, node: Node): Unit =
    onKeyPath(adder)((leaf, key) => {
      leaf.v = Some(v)
      notifyChange(fullKey, leaf.v, leaf)
    }, (node, key, rest) => notifyChange(fullKey, Some(v), node))(key, node)

  // remove fullKey from the prefix tree
  protected def inDel(fullKey: String, key: List[String], node: Node): Unit =
    onKeyPath(finder)((leaf, key) => {
      leaf.v = None
      notifyChange(fullKey, leaf.v, leaf)
    }, (node, key, rest) => {
      finder(rest.head, node).foreach(c=> if (c.v.isEmpty && c.childs.isEmpty && c.listeners.isEmpty) node.childs -= c)
      notifyChange(fullKey, None, node)
    })(key, node)

  // notify listener about all non-None sub-keys
  protected def notifyDownwards(listener: Node, node: Node, keyPrefix: String): Unit = {
    if (node.v != None) notifyChange(keyPrefix, node.v, listener)
    node.childs.foreach(c=>notifyDownwards(listener, c, keyPrefix + "." + c.key))
  }

  // registers a listener on fullKey, calling it with all existing info if update is set
  protected def inOnChange(update: Boolean, fullKey: String, key: List[String], effect: (String, Option[A]) => Unit, node: Node) = {
    onKeyPath(adder)((leaf, key) => {
      leaf.listeners += Left(effect)
      stopMap = stopMap + (effect, () => {
        findKey(key).foreach(_.listeners -= Left(effect))
        stopMap = stopMap - effect
      })
      leaf.childs.foreach(_=>{})
      if (update) notifyDownwards(leaf, leaf, fullKey)
    }, (_,_,_)=>{})(key, node)
    effect
  }

  protected def inOnBatch(fullKey: String, key: List[String], effect:(List[(String, Option[A], Op)])=>Unit, node: Node) = {
      onKeyPath(adder)((leaf, key) => {
      leaf.listeners += Right(effect)
      stopMap = stopMap + (effect, () => {
        findKey(key).foreach(_.listeners -= Right(effect))
        stopMap = stopMap - effect
      })
      leaf.childs.foreach(_=>{})
    }, (_,_,_)=>{})(key, node)
    effect
  }
  
  class Batch(val fullKey:String, val g:AsyncStore[A]) {
    var actions:List[(String, Option[A], Op)] = List()
    def put(key:String, v:A) = {
      actions = (key, Some(v), Add()) :: actions
      this
    }
    def del(key:String) = {
      actions = (key, None   , Del()) :: actions
      this
    }
    def commit = { 
      val changes = actions.reverse
      changes.foreach(_ match {
	      case (key, Some(v), Add()) => g.put(key, v)
	      case (key, _, Del())       => g.del(key)
	    })
	  findKey(fullKey).foreach(_.listeners.foreach(_.right.foreach(_.apply(changes))))
	  this
    }
  }
  
  def findKey(key: String): Option[Node] = inFindKey(key.split("\\.").toList, root)
  
  abstract class Op
  case class Add extends Op
  case class Del extends Op
  
  def batch(key:String) = new Batch(key, this) 
  def onBatch(key:String, effect:(List[(String, Option[A], Op)])=>Unit) = inOnBatch(key, key.split("\\.").toList, effect, root)
  def onChange(key: String, effect: (String, Option[A]) => Unit) = inOnChange(true, key, key.split("\\.").toList, effect, root)
  def onChangeNoUpdate(key: String, effect: (String, Option[A]) => Unit) = inOnChange(false, key, key.split("\\.").toList, effect, root)
  def put(key: String, v: A) = inPut(key, key.split("\\.").toList, v, root)
  def del(key: String) = inDel(key, key.split("\\.").toList, root)
  def stop(effect: (String, Option[A]) => Unit) = stopMap.get(effect).foreach(_.apply())
}

object tester {

  def main(args: Array[String]) {

    val g = new AsyncStore[Int]

    val e1 = g.onChange("store.shelf", (key, v) => println("change on shelf: " + key + " " + v))

    g.put("store", 4)
    g.put("store.shelf.top", 7)

    val storeListener = g.onChange("store", (key, v) => println("change on store: " + key + " " + v))

    g.del("store.closet")
    
    g.onBatch("store", (l)=>println (l.foldLeft("batch : \n")(_+"\t"+_.toString + "\n")))
    
    g.put("store.shelf.top.1", 100)
    
    g.batch("store")
       .put("store.room.1", 2)
       .put("store.shelf.top.middle", 10)
       .del("store.room.1")
       .commit

    g.stop(storeListener)
    
    g.put("store.shelf", 6)
  }
}