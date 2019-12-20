import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartition<K, V> extends Partitioner<Text, DoubleWritable> {
	public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
		String word = key.toString();
		char letter = word.toUpperCase().charAt(0);
		int partitionnumber = 0;
		switch(letter){
		case 'a': case 'A': partitionnumber = 1; break;
		case 'b': case 'B': partitionnumber = 2; break;
		case 'c': case 'C': partitionnumber = 3; break;
		case 'd': case 'D': partitionnumber = 4; break;
		case 'e': case 'E': partitionnumber = 5; break;
		case 'f': case 'F': partitionnumber = 6; break;
		case 'g': case 'G': partitionnumber = 7; break;
		case 'h': case 'H': partitionnumber = 8; break;
		case 'i': case 'I': partitionnumber = 9; break;
		case 'j': case 'J': partitionnumber = 10; break;
		case 'k': case 'K': partitionnumber = 11; break;
		case 'l': case 'L': partitionnumber = 12; break;
		case 'm': case 'M': partitionnumber = 13; break;
		case 'n': case 'N': partitionnumber = 14; break;
		case 'o': case 'O': partitionnumber = 15; break;
		case 'p': case 'P': partitionnumber = 16; break;
		case 'q': case 'Q': partitionnumber = 17; break;
		case 'r': case 'R': partitionnumber = 18; break;
		case 's': case 'S': partitionnumber = 19; break;
		case 't': case 'T': partitionnumber = 20; break;
		case 'u': case 'U': partitionnumber = 21; break;
		case 'v': case 'V': partitionnumber = 22; break;
		case 'w': case 'W': partitionnumber = 23; break;
		case 'x': case 'X': partitionnumber = 24; break;
		case 'y': case 'Y': partitionnumber = 25; break;
		case 'z': case 'Z': partitionnumber = 26; break;
		default: partitionnumber = 0; break;

		}
		return partitionnumber;
		
		
	}
}