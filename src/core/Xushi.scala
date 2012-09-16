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
  
  def interested(key: String) = peers.filter((p) => diceKey(key).exists(p._2.contains(_))).map(_._1)

  def add(key: String, v: Any) = {
    diceKey(key).foreach((mk)=> store.get(mk).getOrElse{store = store + ((mk, None))})
    store = store + ((key, Some(v)))
    interested(key) foreach (send(_)(key, v))
  }

  val send = (to: StoreConnection)=>(x:(String, Any))=> x match {
    case (key, v) => {
    	println("sending to " + to.getID() + " " + key + " " + v)
    	to sendTCP (new ADD(key, v))
    }
  } 
    
  val itemsForUpdate = (key:String)=>store.from(key).takeWhile(_._1.startsWith(key)).filter(_._2.isDefined).map((x)=>(x._1, x._2.get))

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
      
      def removeFirst[A](num: A, list: List[A]) = list diff List(num)
      
      override def received(c: Connection, obj: Object) {
        val conn = c.asInstanceOf[StoreConnection]
        obj match {
          case c: CMD => c match {
            case ADD(key, v) => add(key, v)
            case LISTEN(key) => {
              peers = peers + ((conn, key :: peers.get(conn).getOrElse(List())))
              itemsForUpdate(key).foreach(send(conn))          
            }
            case UNLISTEN(key)=>{
              val v = peers.get(conn)
              v.foreach{(x)=>peers = peers + ((conn, removeFirst(key, x)))}
            }
          }
          case _=>
        }
      }
    })
  }
}


object Xushi {
  def main(args: Array[String]) {
    val a = new Xushi;
    a.init
  }
}