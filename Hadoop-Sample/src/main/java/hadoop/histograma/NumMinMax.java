package hadoop.histograma;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Calcula el minimo y el maáximo de una lista de valores
 * El fichero tiene el siguiente formato:<br>
 * <code>
 * [number] 
 * <code>
 * <br>
 * number el formato espereado es float
 * <br>
 * La salida es un fichero con el un par de valores
 * minValue maxValue
 */
public class NumMinMax extends Configured implements Tool {

	public static class ParseMap extends Mapper<LongWritable, Text, IntWritable ,NumMinMaxWritable> {

		IntWritable outkey = new IntWritable();
		NumMinMaxWritable outValue = new NumMinMaxWritable();

		@Override
		public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
			String fields = line.toString();
			int one = Integer.valueOf(1);
            
			outkey.set(one);
			outValue.setNumber1(Integer.valueOf(fields));
			outValue.setNumber2(Integer.valueOf(fields));
			
			context.write(outkey, outValue);

		}

	}
	
	public static class MinMaxCombiner extends Reducer<IntWritable, NumMinMaxWritable, IntWritable, NumMinMaxWritable>{

		IntWritable outkey = new IntWritable();
		NumMinMaxWritable outValue = new NumMinMaxWritable();
		
		protected void combiner(IntWritable numberOne, Iterable<NumMinMaxWritable> listOfNumbers, Context context) 
	  			throws IOException ,InterruptedException {

			float maxNumber = Float.MIN_VALUE;
			float minNumber = Float.MAX_VALUE;


			for (NumMinMaxWritable tuple : listOfNumbers) {
				// Podemos obtener el primer número o el segundo, ambos son iguales
				float actual = (Float) tuple.getNumber1();
				// Para cada uno de los elementos de la lista, se va obteniendo el mínimo y el máximo
				maxNumber = Math.max(maxNumber, actual);
				minNumber = Math.min(minNumber, actual);
			}
			int one = Integer.valueOf(1);
            
			outkey.set(one);
			outValue.setNumber1(minNumber);
			outValue.setNumber2(maxNumber);

			context.write(outkey, outValue);
		}
	};


	public static class MinMaxReducer extends Reducer<IntWritable, NumMinMaxWritable, IntWritable, NumMinMaxWritable>{
		IntWritable outkey = new IntWritable();
		NumMinMaxWritable outValue = new NumMinMaxWritable();
		
		protected void reduce(IntWritable numberOne, Iterable<NumMinMaxWritable> listOfNumbers, Context context) 
	  			throws IOException ,InterruptedException {

			float maxNumber = Float.MIN_VALUE;
			float minNumber = Float.MAX_VALUE;


			for (NumMinMaxWritable tuple : listOfNumbers) {
				// Podemos obtener el primer número o el segundo, ambos son iguales
				float actual = (Float) tuple.getNumber1();
				// Para cada uno de los elementos de la lista, se va obteniendo el mínimo y el máximo
				maxNumber = Math.max(maxNumber, actual);
				minNumber = Math.min(minNumber, actual);
			}
			int one = Integer.valueOf(1);
			
			outkey.set(one);
			outValue.setNumber1(minNumber);
			outValue.setNumber2(maxNumber);

			context.write(outkey, outValue);
		}
	};

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.println("Número de argumentos incorrectos\n\n"
							+ "Uso: numMinMax <input_path> <output_path>\n\n");
			return -1;
		}
		String input = args[0];   //"data/lista/lista.txt";
		String output = args[1];  //"lista-ord";



		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "NumMinMax");
		
		job.setJarByClass(NumMinMax.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NumMinMaxWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NumMinMaxWritable.class);
		
		job.setMapperClass(ParseMap.class);
		job.setCombinerClass(MinMaxCombiner.class); // Reduce trafico con el combiner
		job.setReducerClass(MinMaxReducer.class); 		
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new NumMinMax(), args);
	}
}
