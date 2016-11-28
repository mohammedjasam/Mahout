package mahout;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import java.io.BufferedReader;
import java.io.FileReader;

class ConvertToSeq {

	private ConvertToSeq() {}

	public static final int NUM_COLUMNS = 3;


	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception 
	{
		
		String INPUT_FILE =  "/usr/tmp/mahout/centroid.csv";
		String OUTPUT_FILE = "/usr/tmp/mahout/clus";
		
		List<NamedVector> apples = new ArrayList<NamedVector>();
		NamedVector apple;
		BufferedReader br = null;
		br = new BufferedReader(new FileReader(INPUT_FILE));
		String sCurrentLine;
		while ((sCurrentLine = br.readLine()) != null) {
		
			String item_name = sCurrentLine.split(",")[0];
			double[] features = new double[NUM_COLUMNS-1];
			for(int indx=1; indx<NUM_COLUMNS;++indx){
				features[indx-1] = Float.parseFloat(sCurrentLine.split(",")[indx]);	
			}
		
			apple = new NamedVector(new DenseVector(features), item_name );
			apples.add(apple);
	}

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);
	Path path = new Path(OUTPUT_FILE);

	@SuppressWarnings("deprecation")
	SequenceFile.Writer writer = new SequenceFile.Writer(fs,  conf, path, Text.class, VectorWritable.class);

	VectorWritable vec = new VectorWritable();
	for(NamedVector vector : apples){
	vec.set(vector);
	writer.append(new Text(vector.getName()), vec);
	}
	writer.close();
	
	@SuppressWarnings("deprecation")
	SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT_FILE), conf);

	Text key = new Text();
	VectorWritable value = new VectorWritable();
	while(reader.next(key, value)){
		System.out.println(key.toString() + " , " + value.get().asFormatString());
	}
		reader.close();	
	}
}