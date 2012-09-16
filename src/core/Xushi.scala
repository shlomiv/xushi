package core
import scala.collection.immutable.HashMap
import com.esotericsoftware.kryonet.Listener
import com.esotericsoftware.kryonet.Connection
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.collection.immutable.TreeMap

class Xushi {
  var store = new TreeMap[String, Option[Any]]
  var peers = new HashMap[StoreConnection, List[String]]

  def diceKey(key:String)=key.split("\\.").inits.filterNot(_.isEmpty).map((x) => x reduce (_.concat(".") ++ _)).toStream.reverse
  
  def interested(key: String) = {
    val diced = diceKey(key)
    peers.filter((p) => diced.exists(p._2.contains(_))).map(_._1)
  }

  def add(key: String, v: Any) = {
    diceKey(key).foreach((mk)=> store.get(mk).getOrElse{store = store + ((mk, None))})
    store = store + ((key, Some(v)))
    interested(key) foreach (send(_)(key, v))
  }

  val send = (to: StoreConnection)=>(x:(String, Any))=> x match {
    case (key, v) => {
    	println("sending to " + to.getID() + " " + key + " " + v)
    	to.sendTCP(new ADD(key, v))
    }
  } 
    
  val getAllItemsFromStore = (key:String)=>{  
	  val head = key.split("\\.",1)(0)
	  store.from(head).takeWhile(_._1.startsWith(head)).filter(_._2.isDefined).map {case (k, Some(v))=>(k,v)}
  }

  val server = new StoreServer(9999)

  def init = {
    server.accept()
    server.server.addListener(new Listener {
      override def connected(c: Connection) {
        val conn = c.asInstanceOf[StoreConnection]
        peers = peers + ((conn, List()))
      }
      override def disconnected(c: Connection) {
        val conn = c.asInstanceOf[StoreConnection]
        peers = peers - conn
      }
      override def received(c: Connection, obj: Object) {
        val conn = c.asInstanceOf[StoreConnection]
        obj match {
          case c: CMD => c match {
            case ADD(key, v) => add(key, v)
            case LISTEN(key) => {
              peers = peers + ((conn, peers.get(conn).getOrElse(List()).::(key)))
              getAllItemsFromStore(key).foreach(send(conn))          
              }
          }
          case _=>
        }

      }
    })
  }

  //def onChange(key:String, e:(String, Any)=>Unit) = null

}


object Xushi {
  def main(args: Array[String]) {
 //   var t = new TreeMap[String, Int]
 //   t = t + ("a" -> 1) + ("b"->2) + ("a.1"->3) + ("a.2"->4) + ("a.2.t"->5)+("b.1"->6)
 //   t.takeWhile(_._1.startsWith("b")).foreach(println _)
 val a = new Xushi;
    // a.peers = a.peers + ("a" -> List("key1", "key2")) + ("b" -> List("key1.key6", "key2.key5"))
    //a.add("key1.key6.k1.2.34.5.6.778.fsfg.sd.er", 23)
    a.init
  }
}