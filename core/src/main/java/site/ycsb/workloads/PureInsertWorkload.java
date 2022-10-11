package site.ycsb.workloads;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.RandomByteIterator;
import site.ycsb.Status;
import site.ycsb.Workload;
import site.ycsb.WorkloadException;

/**
 * A workload that only does database insertion.
 */
public class PureInsertWorkload extends Workload {
  private static final long LENGTH_PAYLOAD = 100;
  protected String table;

  @Override
  public void init(Properties p) throws WorkloadException {
    table = p.getProperty(CoreWorkload.TABLENAME_PROPERTY, CoreWorkload.TABLENAME_PROPERTY_DEFAULT);
    System.out.println("init, table: " + table);
  }

  @Override
  public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {
    System.out.format("init thread %d, total %d\n", mythreadid, threadcount);
    try {
      return new ThreadState(p, mythreadid, threadcount);
    } catch (IOException e) {
      System.out.println(e);
      throw new WorkloadException(e.getMessage());
    }
  }

  @Override
  public boolean doInsert(DB db, Object threadstate) {
    final String key = ((ThreadState) threadstate).getNextKey();
    final HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

    values.put("v", new RandomByteIterator(LENGTH_PAYLOAD));

    if (db.insert(table, key, values) == Status.OK) {
      return true;
    }
    return false;
  }

  @Override
  public boolean doTransaction(DB db, Object threadstate) {
    return doInsert(db, threadstate);
  }

  protected class ThreadState {
    private final int threadID;
    private FileInputStream workloadStream;
    private Scanner workloadScanner;

    protected ThreadState(final Properties p, final int threadID, final int threadCount) throws IOException {
      this.threadID = threadID;
      final String filepath = p.getProperty("workloadpath", ".");
      final String filename = String.format("run.w.%d", this.threadID + 1);

      workloadStream = new FileInputStream(Paths.get(filepath, filename).toAbsolutePath().toString());
      workloadScanner = new Scanner(workloadStream, "UTF-8");
    }

    protected String getNextKey() {
      if (!workloadScanner.hasNext()) {
        // If reached EOF, restart from beginning.
        workloadScanner = new Scanner(workloadStream);
      }
      return workloadScanner.nextLine().split(" ")[1];
    }
  }
}
