package fh.its.bde.wordcount.MR;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LongInverseComparator extends WritableComparator {

	private static final Log LOG = LogFactory.getLog(LongInverseComparator.class);

	public LongInverseComparator() {
		super(LongWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		LOG.info("--------------Comparing");
		LOG.info(a);
		LOG.info(b);
		return -((LongWritable) a).compareTo((LongWritable) b);

	}
}