package hadoop.histograma;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NumMinMaxWritable implements
		WritableComparable<NumMinMaxWritable> {

	private float number1;
	private float number2;

	public NumMinMaxWritable() {
	}

	public NumMinMaxWritable(float number1, float number2) {
		setNumber1(number1);
		setNumber2(number2);
	}

	public void setNumber1(float number1) {
		this.number1 = number1;
	}

	public float getNumber1() {
		return this.number1;
	}

	public void setNumber2(float number2) {
		this.number2 = number2;
	}

	public float getNumber2() {
		return this.number2;
	}

	@Override
	public String toString() {
		return (number1 + " " + number2);
	}

	@Override
	public int hashCode() {
		String cadena = Float.toString(number1) + Float.toString(number2);
		return cadena.hashCode();
	}

	// ******************************************************************************
	// Método que sirve para indicar si son iguales dos objetos del tipo del writable 
	// ******************************************************************************
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof NumMinMaxWritable))
			return false;
		NumMinMaxWritable other = (NumMinMaxWritable) o;
		return ((this.number1 == (other.number1)) && (this.number2 == other.number2));
	}

	// *********************
	// * Writable interface
	// *********************
	public void readFields(DataInput in) throws IOException {
		number1 = in.readFloat();
		number2 = in.readFloat();
	}

	public void write(DataOutput out) throws IOException {
		out.writeFloat(number1);
		out.writeFloat(number2);		
	}

	// ******************************************************************************
	// Se compara el primer número y si e
	// Tenemos un par  (number1,number2)
	// Retorna el valor de la primera comparación, salvo si son iguales, 
	// que entonces va a retornar el resultado de la comparación del segundo número.
	// ******************************************************************************
	@Override
	public int compareTo(NumMinMaxWritable o) {
		
		int cmp;

		cmp = (number1<o.number1) ? 1 : ((number1>o.number1) ? -1 : 0);
		
		if (cmp==0){
			return number2<o.number2 ? 1 : ( number2 == o.number2 ? 0 : -1 );
		}
		return (cmp);
		
	}


}
