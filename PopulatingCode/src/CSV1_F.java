import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class CSV1_F {
	public static void main(String[] args) throws FileNotFoundException {

		PrintWriter pw = new PrintWriter(new File("played_in_partF.csv"));
		String[] names = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
				"s", "t", "u", "v", "w", "x", "y", "z" };
		Random rand = new Random();
		Set<String> records = new HashSet<>();
		String m = "";
		for (int i = 0; i < 9977272; i++) {
			StringBuilder sb = new StringBuilder();
			int match = rand.nextInt(9977272);
			int r = rand.nextInt(26);
			int position = rand.nextInt(5);
			while (true) {
				if (records.contains(names[r] + match)) {
					match = rand.nextInt(9977272);
				}

				else {
					break;
				}
			}
			sb.append(match);
			sb.append(',');
			sb.append(names[r]);
			sb.append(',');
			sb.append(2018);
			sb.append(',');
			sb.append(position);
			sb.append('\n');
			records.add(names[r] + match);
			pw.write(sb.toString());
		}
		for (int i = 0; i < 22728; i++) {
			StringBuilder sb = new StringBuilder();
			sb.append(i);
			sb.append(',');
			sb.append("pele");
			sb.append(',');
			sb.append(2018);
			sb.append(',');
			sb.append(4);
			sb.append('\n');
			pw.write(sb.toString());
		}
		pw.close();
		System.out.println("done!");
	}
}
