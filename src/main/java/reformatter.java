import java.io.*;

public class reformatter {
    public static void main(String[] args) {
        // The name of the file to open.
        String fileName = "res/correlations/normalize/correlations/corr_topK_state_opioid/part-00000";
        String writefile = "res/correlations/normalize/correlations/corr_topK_state_opioid/reformatted";

        // This will reference one line at a time
        String line = null;

        try {
            // FileReader reads text files in the default encoding.
            FileReader fileReader =
                    new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
                    new BufferedReader(fileReader);

            FileWriter fileWriter =
                    new FileWriter(writefile);

            // Always wrap FileWriter in BufferedWriter.
            BufferedWriter bufferedWriter =
                    new BufferedWriter(fileWriter);

            while((line = bufferedReader.readLine()) != null) {
                String[] corrs = line.split(",");
                bufferedWriter.write(corrs[0]+" "+corrs[1]+","+corrs[2]+" "+corrs[3]+","+corrs[4]);
                bufferedWriter.newLine();

            }

            // Always close files.
            bufferedReader.close();

            // Always close files.
            bufferedWriter.close();
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
            // Or we could just do this:
            // ex.printStackTrace();
        }
    }
}
