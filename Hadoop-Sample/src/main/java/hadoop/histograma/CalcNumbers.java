package hadoop.histograma;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
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

public class CalcNumbers extends Configured implements Tool {

	public static class ParseMap extends
			Mapper<LongWritable, Text, IntWritable, IntWritable> {

		float minValue;
		float maxValue;
		int nBars;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			// Estos valores se obtienen del context, escritos en el método run
			// de esta clase.

			Configuration conf = context.getConfiguration();
			nBars = Integer.parseInt(conf.get("numBarras"));
			minValue = Float.parseFloat(conf.get("numMin"));
			maxValue = Float.parseFloat(conf.get("numMax"));

		}

		protected void map(LongWritable offset, Text line, Context context)
				throws IOException, InterruptedException {

			IntWritable outKey = new IntWritable();
			IntWritable outValue = new IntWritable();
			int res1;

			String fields = line.toString();

			float myNumber = new Float(fields);
			if (myNumber == maxValue){
				res1 = Math.abs(nBars-1);
				
			}else{	
				float res = (myNumber - minValue) / ((maxValue - minValue) / nBars);
				res1 = (int) Math.ceil(Math.floor((double) res));
				
			}
			
			

			outKey.set(res1);
			outValue.set(1);

			context.write(outKey, outValue);
		}

	}

	public static class CalcReducer extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		protected void reduce(IntWritable bar, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {

			int total = 0;
			IntWritable outValue = new IntWritable();

			for (IntWritable tuple : counts) {
				total += tuple.get();
			}

			outValue.set(total);
			context.write(bar, outValue);
		}
	};

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			System.out
					.println("Parámetros incorrectos\n\n"
							+ "Uso: calcNumbers <input_path> <input2_path> <output_path> <numberBars>\n\n");
			return -1;
		}
		// Valores asignados en pruebas.
		String input = args[0]; // "data/lista/lista.txt";
		String input2 = args[1]; // "lista-ord/part-r-00000";
		String output = args[2]; // "lista-final";
		String barras = args[3]; // "4";


		// Obtiene del fichero generado por el primer job los valores minimo y
		// maximo
		Values values = getMaxMinFromFile(input2);

		// Se establedcen los valores en la configuración para usarlos cuando
		// los necesite en el mapper
		Configuration conf = new Configuration();

		conf.set("numBarras", barras);
		conf.set("numMin", Float.toString(values.getMin()));
		conf.set("numMax", Float.toString(values.getMax()));

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(CalcNumbers.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(ParseMap.class);

		job.setReducerClass(CalcReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));

		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

		return 0;
	}

	// Metodo que sirve para obtener los valores maximo y minimo
	public Values getMaxMinFromFile(String input) {
		Path mPath = new Path(input);
		FileSystem fs = null;
		String line = null;
		BufferedReader br = null;
		Values newValues = null;
		float min;
		float max;

		try {
			fs = FileSystem.get(new Configuration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			br = new BufferedReader(new InputStreamReader(fs.open(mPath)));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			line = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (line != null) {
			// La linea es de la forma (num tabulador num espacio num)
			// Se separa por el tabulador
			String[] fields1 = line.split("[\\t]");

			// No se usa el primer valor fields1[0] que contiene un valor que no
			// interesa
			// Se usa el segundo que contiene el par min max
			String[] fields = fields1[1].split(" ");
			min = Float.parseFloat(fields[0]);
			max = Float.parseFloat(fields[1]);
			newValues = new Values(min, max);
			return (newValues);
		} else {
			System.out.println("Error en lectura \n\n"
					+ "No se pueden hallar los valores Mínimos y Máximos\n\n");
			System.exit(-1);
		}

		return (null);
	}

	public static class Values {
		private float maxValue;
		private float minValue;

		public Values(float minValue, float maxValue) {
			this.minValue = minValue;
			this.maxValue = maxValue;
		}

		public float getMax() {
			return this.maxValue;
		}

		public void setMax(float maxValue) {
			this.maxValue = maxValue;
		}

		public float getMin() {
			return this.minValue;
		}

		public void setMin(float minValue) {
			this.minValue = minValue;
		}
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new CalcNumbers(), args);
	}
}
