package core
import scala.collection.immutable.SortedMap
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryonet.Client
import com.esotericsoftware.kryonet.Connection
import com.esotericsoftware.kryonet.KryoSerialization
import com.esotericsoftware.kryonet.Listener
import com.esotericsoftware.kryonet.Server
import com.esotericsoftware.kryo._
import scala.collection.immutable.TreeMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.HashMap

abstract class Store[A](val port: Int) {
  val kryi = new Kryo

  object CmdSerialization {
    def register(k: Kryo) {
      k.register(classOf[ADD], new ADDSerializer)
      k.register(classOf[LISTEN], new LISTENSerializer)
      k.register(classOf[UNLISTEN], new UNLISTENSerializer)
      k.register(classOf[DEL], new DELSerializer)
    }

    class LISTENSerializer extends Serializer[LISTEN] {
      override def read(kryo: Kryo, input: Input, t: Class[LISTEN]): LISTEN = LISTEN(input.readString(), input.readString())
      override def write(kryo: Kryo, output: Output, v: LISTEN) = {
        output.writeString(v.key)
        output.writeString(v.what)
      }
    }
    class UNLISTENSerializer extends Serializer[UNLISTEN] {
      override def read(kryo: Kryo, input: Input, t: Class[UNLISTEN]): UNLISTEN = UNLISTEN(input.readString(),input.readString())
      override def write(kryo: Kryo, output: Output, v: UNLISTEN) = {
    	  output.writeString(v.key)
    	  output.writeString(v.what)
      }
    }
    class DELSerializer extends Serializer[DEL] {
      override def read(kryo: Kryo, input: Input, t: Class[DEL]): DEL = DEL(input.readString())
      override def write(kryo: Kryo, output: Output, v: DEL) = output.writeString(v.key)
    }
    class ADDSerializer extends Serializer[ADD] {
      override def read(kryo: Kryo, input: Input, t: Class[ADD]): ADD = ADD(input.readString(), kryo.readClassAndObject(input))
      override def write(kryo: Kryo, output: Output, v: ADD) = {
        output.writeString(v.key)
        kryo.writeClassAndObject(output, v.v);
      }
    }
  }

  {
    CmdSerialization.register(kryi)
  }

}

abstract class CMD
	case class ADD(key: String, v: Any) extends CMD
	case class DEL(key: String) extends CMD
	case class LISTEN(key: String, what:String) extends CMD
	case class UNLISTEN(key:String,what:String) extends CMD


class StoreClient(port: Int) extends Store(port) {
  val client = new Client(8192, 2048, new KryoSerialization(kryi))

  val connect = () => {
    println("connecting");
    client.start();
    client.connect(500000, "localhost", port);
    client.addListener(new Listener {
      override def received(c: Connection, obj: Object) {
      }
    })
  }

}

object StoreServerTester {
  val client = new StoreClient(9999)
	
  def main(args: Array[String]) {
    //	  val StoreServer = new StoreServer(1234)
    //	  StoreServer.accept()

    var listeners = new TreeMap[String, List[ScalaObject]]
    //var listeners = new 


    client.connect()

    client.client.addListener(new Listener {
      override def connected(c: Connection) {
      }
      override def disconnected(c: Connection) {
      }
      override def received(c: Connection, obj: Object) = obj match {
          case c: CMD => c match {
            case ADD(key, v) => diceKey(key).foreach{(k)=>disptcher.get(k).foreach(_ foreach (_._2(key, v)))}
          }
          case _ =>
        }
    })

    def diceKey(key:String)=key.split("\\.").inits.filterNot(_.isEmpty).map((x) => x reduce (_.concat(".") ++ _)).toStream.reverse
  
    
   /* client.client.sendTCP(new LISTEN("bss"))
    client.client.sendTCP(new LISTEN("bss"))
    client.client.sendTCP(new LISTEN("bss"))
    client.client.sendTCP(new UNLISTEN("bss"))
    client.client.sendTCP(new LISTEN("bss"))
    client.client.sendTCP(new UNLISTEN("bss"))
    client.client.sendTCP(new UNLISTEN("bss"))
    client.client.sendTCP(new UNLISTEN("bss"))
    client.client.sendTCP(new LISTEN("ass.1.3"))
    client.client.sendTCP(new ADD("bss.1.2.3.4", 45))
    client.client.sendTCP(new ADD("ass.1.2.3.4", 45))
    client.client.sendTCP(new ADD("ass.1.3.3.4", 45))
    client.client.sendTCP(new LISTEN("ass"))
*/
   // Thread.sleep(3000)
    //onChange("bss.3") {(x,y)=>println ("1 " + x + " -> " + y)}
    val i = onChange("bss") {(x,y)=>println ("3 "+x + " -> " + y)}
    onChange("bss") {(x,y)=>println ("2 "+x + " -> " + y)}
    Thread.sleep(3000)
    client.client.sendTCP(new ADD("bss.1.2.3.3", 1))
    client.client.sendTCP(new ADD("bss.2.2.3.3", 2))
    client.client.sendTCP(new ADD("bss.3.2.3.3", 3))
        Thread.sleep(3000)

    stop(i)
    client.client.sendTCP(new ADD("bss.3.3.3.3", 4))
    client.client.sendTCP(new ADD("bss.3.3.3.4", 5))
    client.client.sendTCP(new ADD("bss.1.2.3.3", 6))
    Thread.sleep(3000)
    onChange("bss") {(x,y)=>println ("4 "+x + " -> " + y)}
    Thread.sleep(5000000)
  }
  
  val ai = new AtomicInteger(0)
  var disptcher = new HashMap[String, HashMap[String, (String, Any)=>Unit]]
  val onChange = (key : String) => (e:(String, Any)=>Unit) => {
	val uniqueKey = key + "%ADD%" + ai.getAndIncrement()
	val l = disptcher.get(key).getOrElse(new HashMap)
	disptcher = disptcher + ((key, l+ ((uniqueKey, e))))

	client.client.sendTCP(new LISTEN(key, "ADD"))
    uniqueKey
  }
  val stop = (lid:String)=>{
    val (key, what) = {val r = lid.split("%",3);(r(0),r(1))}
    disptcher.get(key).map(_-lid).map((x)=>disptcher = disptcher + ((key, x)))
    disptcher.get(key).foreach((x)=>if (x.isEmpty) disptcher = disptcher - key)
    client.client.sendTCP(new UNLISTEN(key, what))
    Unit
  }
}