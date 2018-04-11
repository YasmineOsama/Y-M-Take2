import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Random;

public class CSV {
	public static void main(String[] args) throws FileNotFoundException {

		PrintWriter pw = new PrintWriter(new File("cup_matches.csv"));
		StringBuilder sb = new StringBuilder();
		sb.append("mid");
		sb.append(',');
		sb.append("round");
		sb.append(',');
		sb.append("year");
		sb.append(',');
		sb.append("num_ratings");
		sb.append(',');
		sb.append("rating");
		sb.append('\n');
		
		String [] rounds = {"32nd","16th","quarter_final","SemiFinal","Final"};
		
		Random rand = new Random();
		
		for (int i = 0; i < 2680; i++) {
			
			int  round = rand.nextInt(4)+1;
			int  num_rating = rand.nextInt(9000)+1000;
			int  rating = rand.nextInt(10)+1;

			
			sb.append(i);
			sb.append(',');
			sb.append(rounds[round]);
			sb.append(',');
			sb.append(2018);
			sb.append(',');
			sb.append(num_rating);
			sb.append(',');
			sb.append(rating);
			sb.append('\n');
			
		}
		pw.write(sb.toString());
		pw.close();
		System.out.println("done!");
	}
}