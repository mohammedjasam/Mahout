package mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;

public class SeqToText {
	public static void main(String[] args) throws Exception {

		String OUTPUT_FILE = "/usr/tmp/mahout/output/clusters-1-final/part-r-00000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		// Path path = new Path(OUTPUT_FILE);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				OUTPUT_FILE), conf);

		Text key = new Text();
		VectorWritable value = new VectorWritable();
		boolean b = reader.next(key, value);
		try {
			while (reader.next(key, value)) {
				System.out.println(key.toString() + " , "
						+ value.get().asFormatString());
			}
		} catch (Exception e) {
			System.out.println(e.toString());

		}
	}
}
