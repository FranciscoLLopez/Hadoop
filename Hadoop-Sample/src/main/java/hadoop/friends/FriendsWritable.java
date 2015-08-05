package hadoop.friends;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*  FriendsWritable
 * Se pretende construir un writable con la forma:
 *    ( (boolean, Friend) donde los dos son del tipo String
 *     Por ejemplo:
 *        (true, a)
 *            
 */


public class FriendsWritable implements
		WritableComparable<FriendsWritable> {

	private String myBool;
	private String myFriend;

	public FriendsWritable() {
	}

	public FriendsWritable(String myBool, String myFriend) {
		setMyBool(myBool);
		setMyFriend(myFriend);
	}

	public void setMyBool(String myBool) {
		this.myBool = myBool;
	}

	public String getMyBool() {
		return this.myBool;
	}

	public void setMyFriend(String myFriend) {
		this.myFriend = myFriend;
	}

	public String getMyFriend() {
		return this.myFriend;
	}

	@Override
	public String toString() {
		return (myBool + " " + myFriend);
	}

	// ********************************************
	// * Important methods to override from Object
	// ********************************************

	/**
	 * HashCode must be implemented properly. Otherwise, Hadoop MapReduce won't
	 * work properly, as the {@link Partition} won't route properly tuples to
	 * the corresponding reducer.
	 */
	@Override
	public int hashCode() {
		String cadena = myBool + myFriend;
		return cadena.hashCode();
	}

	/**
	 * Convenient to implement
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof FriendsWritable))
			return false;
		FriendsWritable other = (FriendsWritable) o;
		return ( (this.myBool.equals(other.myBool)) && (this.myFriend.equals(other.myFriend)));
	}

	// *********************
	// * Writable interface
	// *********************
	public void readFields(DataInput in) throws IOException {
		myBool = Text.readString(in);
		myFriend = Text.readString(in);

	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, myBool);
		Text.writeString(out, myFriend);
		
	}

	// *********************
	// * Comparable interface
	// *********************


	@Override
	public int compareTo(FriendsWritable o) {
		
		int cmp = myBool.compareTo(o.myBool);
		
		if (cmp==0){
			return myFriend.compareTo(o.myFriend);
		}
		return (cmp);
	}


}
