package hadoop.friends;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class FriendsMRWritables extends Configured implements Tool {

	public static class friendMap extends
			Mapper<LongWritable, Text, Text, FriendsWritable> {

		private Text word1 = new Text();
		FriendsWritable outValue1 = new FriendsWritable();
		private Text word2 = new Text();
		FriendsWritable outValue2 = new FriendsWritable();

		@Override
		protected void map(LongWritable offset, Text line, Context context)
				throws IOException, InterruptedException {
			// La linea entran pares de valores, por ejemplo:
			// A B
			String row[] = line.toString().split(" ");
			String friendFrom = row[0];  // En el caso ejemplo, tendría el valor A
			String friendTo = row[1];    // En el caso ejemplo, tendría el valor B
			String trueRel = new String("true");
			String falseRel = new String("false");

			// Se construye 
			word1.set(friendFrom); // Key
			// Se devuelve el valor A como clave
			
			outValue1.setMyBool(trueRel);    // Value
			outValue1.setMyFriend(friendTo); // Value
			// Se devuelve como valor el writable (true,B)

			context.write(word1, outValue1); // Se emite (A,(true,B))

			word2.set(friendTo);               // Construimos key B
			outValue2.setMyBool(falseRel);     // 
			outValue2.setMyFriend(friendFrom);
			// Se devuelve como valor el writable (false,A)

			context.write(word2, outValue2); // Se emite (B,(false,A))

		};
	}

	public static class MaxReducer extends
			Reducer<Text, FriendsWritable, Text, Text> {

		protected void reduce(Text friendFrom,
				Iterable<FriendsWritable> relations, Context context)
				throws IOException, InterruptedException {

			List<String> directRel = new ArrayList<String>();
			List<String> reverseRel = new ArrayList<String>();

			// Se recorre la lista de elementos con la key A
			// (A,[(true,B),(true,C)....]
			// Se guarda en dos listas, una de relación directa y otra de relación inversa
			for (FriendsWritable relation : relations) {
				if (relation.getMyBool().equals("true")) { 
					directRel.add(relation.getMyFriend());
				} else {
					reverseRel.add(relation.getMyFriend());
				}

			}
			
			// Se recorre la lista de elementos de relaciones inversas con los elementos 
			// de relaciones directas y se obtienen de aquí los pares que se desean
			for (int i = 0; i < reverseRel.size(); i++) {
				for (int j = 0; j < directRel.size(); j++) {
					Text word1 = new Text();
					Text word2 = new Text();

					word1.set(reverseRel.get(i));
					word2.set(directRel.get(j));

					context.write(word1, word2);
				}
			}

		};

	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out
					.println("INúmero de parámetros incorrectos\n\n"
							+ "Uso: friendsMRWritable <input_path> <output_path>\n\n");
			return -1;
		}
		String input = args[0]; // "data/friends/friends.txt";
		String output = args[1]; // "lista_sal";

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		@SuppressWarnings("deprecation")
		Job job = new Job(getConf(), "Los amigos de mis amigos");
		job.setJarByClass(FriendsMRWritables.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FriendsWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(friendMap.class);

		job.setReducerClass(MaxReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new FriendsMRWritables(), args);
	}
}
