import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.File
import org.awandb.core.engine.{AwanTable, EngineGovernor}
import java.util.concurrent.atomic.AtomicInteger

// Adjust imports based on your actual package structure
// import org.awandb.core.AwanTable
// import org.awandb.core.jni.NativeBridge

// ==================================================================================
// MOCK GOVERNOR
// ==================================================================================
class TestQuotaGovernor(val maxOps: Int) extends EngineGovernor {
  private val currentOps = new AtomicInteger(0)
  private val rejectedOpsCounter = new AtomicInteger(0)

  override def canAcceptInsert(tenantId: String): Boolean = {
    if (currentOps.get() >= maxOps) {
      rejectedOpsCounter.incrementAndGet()
      false 
    } else {
      currentOps.incrementAndGet()
      true
    }
  }

  override def recordUsage(ops: Int, bytes: Long): Unit = {}
  
  def getRejectedCount: Int = rejectedOpsCounter.get()
}

class BufferPoolEvictionSpec extends AnyFlatSpec with Matchers {

  "AwanDB BufferPool" should "not segfault when the pacemaker sweeps after a table is dropped" in {
    val TEST_DIR = "data/eviction_test_data"
    
    // Setup: Ensure a clean directory
    val dir = new File(TEST_DIR)
    if (dir.exists()) dir.listFiles().foreach(_.delete())
    dir.mkdirs()

    // 1. Create a table to allocate native blocks
    // Using a high quota governor so we don't trigger the SafetySpec rejections
    val governor = new TestQuotaGovernor(10000)
    val table = new AwanTable("drop_test_table", 100, TEST_DIR, governor)
    table.addColumn("val")

    // 2. Insert enough data to ensure blocks are allocated and registered in the C++ BufferPool
    println("[TEST] Seeding data to allocate native blocks...")
    for (i <- 1 to 500) {
      table.engineManager.submitInsert(i)
    }
    
    // Wait for asynchronous inserts to settle into native memory
    Thread.sleep(1000) 

    // 3. Drop the table
    // IMPORTANT: This method must be the one that invokes NativeBridge.destroyBlockNative
    println("[TEST] Dropping table (triggering physical memory free)...")
    table.drop() // Replace with your actual drop/destroy method if named differently

    // 4. Force the Pacemaker Sweep
    // EXPECTATION BEFORE FIX: The JVM will violently crash here with a SIGSEGV.
    // EXPECTATION AFTER FIX: The sweep will run safely, find no dangling pointers, and pass.
    println("[TEST] Forcing Pacemaker Sweep...")
    try {
      // Trigger the watermarked LRU eviction manually
      // Assuming this is exposed or accessible in test scope
      org.awandb.core.jni.NativeBridge.triggerPacemakerSweep()
      
      // If we reach this line, the JVM survived. The fix is working!
      println("[TEST] Sweep completed successfully. No segfault detected.")
      succeed
    } catch {
      case e: Exception => 
        fail(s"Sweep failed with an unexpected JVM exception: ${e.getMessage}")
    }
  }
}