import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class CSV1 {
	public static void main(String[] args) throws FileNotFoundException {

		PrintWriter pw = new PrintWriter(new File("played_in.csv"));
		StringBuilder sb = new StringBuilder();
		sb.append("mid");
		sb.append(',');
		sb.append("name");
		sb.append(',');
		sb.append("year");
		sb.append(',');
		sb.append("position");
		sb.append('\n');

		String[] names = { "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r",
				"s", "t", "u", "v", "w", "x", "y", "z" };

		Random rand = new Random();
		Set<String> records = new HashSet<>();
		String m = "";

		for (int i = 0; i < 58842; i++) {

			int match = rand.nextInt(2680);
			int r = rand.nextInt(26);
			int position = rand.nextInt(4) + 1;

			while (true) {

				if (records.contains(names[r] + match)) {
					match = rand.nextInt(2680);

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

		}

		for (int i = 0; i < 118; i++) {

			sb.append(i);
			sb.append(',');
			sb.append("pele");
			sb.append(',');
			sb.append(2018);
			sb.append(',');
			sb.append(4);
			sb.append('\n');

		}
		pw.write(sb.toString());
		pw.close();
		System.out.println("done!");
	}
}