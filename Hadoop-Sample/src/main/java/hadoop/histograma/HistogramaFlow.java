package hadoop.histograma;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HistogramaFlow extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		 if (args.length!=4){
		 System.out.println("Número de parámetros incorrectos\n\n" +
		 "Usage: histogramaFlow <input_path> <outputM_path> <output_path> <n_bars_size>\n\n");
		 return -1;
		 }
		String input = args[0]; //"data/lista/lista.txt";
		String inputOutputM = args[1] ; //"lista-ord";
		String output = args[2]; //"lista-final";
		String barras = args[3]; //"5";
		
		// Se lanza el primer job
		ToolRunner.run(new NumMinMax(), new String[] { input, inputOutputM });

		// Se le añade el directorio de salida el nombre del fichero de entrada, para que le segundo job
		// sepa donde recoger los valores minimo y maximo.
		inputOutputM = inputOutputM + "/part-r-00000";
		
		// Se lanza el segundo job
		ToolRunner.run(new CalcNumbers(), new String[] { input, inputOutputM,
				output, barras });

		return 0;

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new HistogramaFlow(), args);

	}

}
