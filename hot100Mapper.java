import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class hot100Mapper
    extends Mapper<LongWritable, Text, NullWritable, Text> {

  private static final int EXPECTED_FIELDS = 8;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    
    if (key.get() == 0) {
      context.write(NullWritable.get(), 
          new Text("Date,Song,Artist,Rank,Last Week,Peak Position,Weeks in Charts"));
      return;
    }
    
    String[] fields = parseCSVLine(line);
    
    if (fields.length != EXPECTED_FIELDS) {
      System.err.println("MALFORMED_RECORD: Expected " + EXPECTED_FIELDS + 
                         " fields, got " + fields.length + " at line " + key.get());
      return;
    }
    
    String date = fields[0].trim();
    String song = fields[1].trim();
    String artist = fields[2].trim();
    String rank = fields[3].trim();
    String lastWeek = fields[4].trim();
    String peakPosition = fields[5].trim();
    String weeksInCharts = fields[6].trim();
    
    if (!isValidDate(date)) {
      System.err.println("INVALID_DATE: " + date + " at line " + key.get());
      return;
    }
    
    if (song.isEmpty() || artist.isEmpty()) {
      System.err.println("MISSING_REQUIRED_FIELD: Song or Artist empty at line " + key.get());
      return;
    }
    
    if (!isValidNumber(rank) || !isValidNumber(peakPosition) || !isValidNumber(weeksInCharts)) {
      System.err.println("INVALID_NUMBER: at line " + key.get());
      return;
    }
    
    if (lastWeek.equals("#") || lastWeek.isEmpty()) {
      lastWeek = "NULL";
    } else if (!isValidNumber(lastWeek)) {
      System.err.println("INVALID_LAST_WEEK: " + lastWeek + " at line " + key.get());
      return;
    }
    
    song = cleanQuotes(song);
    artist = cleanQuotes(artist);
    
    StringBuilder cleanedRecord = new StringBuilder();
    cleanedRecord.append(date).append(",")
                 .append(escapeCSV(song)).append(",")
                 .append(escapeCSV(artist)).append(",")
                 .append(rank).append(",")
                 .append(lastWeek).append(",")
                 .append(peakPosition).append(",")
                 .append(weeksInCharts);
    
    context.write(NullWritable.get(), new Text(cleanedRecord.toString()));
  }
  
  private String[] parseCSVLine(String line) {
    String[] result = new String[8];
    int fieldIndex = 0;
    StringBuilder currentField = new StringBuilder();
    boolean inQuotes = false;
    
    for (int i = 0; i < line.length() && fieldIndex < 8; i++) {
      char c = line.charAt(i);
      
      if (c == '"') {
        inQuotes = !inQuotes;
      } else if (c == ',' && !inQuotes) {
        result[fieldIndex++] = currentField.toString();
        currentField = new StringBuilder();
      } else {
        currentField.append(c);
      }
    }
    
    if (fieldIndex < 8) {
      result[fieldIndex] = currentField.toString();
    }
    
    for (int i = fieldIndex + 1; i < 8; i++) {
      result[i] = "";
    }
    
    return result;
  }
  
  private boolean isValidDate(String date) {
    return date.matches("\\d{4}-\\d{2}-\\d{2}");
  }
  
  private boolean isValidNumber(String str) {
    if (str == null || str.isEmpty()) return false;
    try {
      Integer.parseInt(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
  
  private String cleanQuotes(String str) {
    if (str.startsWith("\"") && str.endsWith("\"")) {
      str = str.substring(1, str.length() - 1);
    }
    return str.replace("\"\"", "\"");
  }
  
  private String escapeCSV(String str) {
    if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
      return "\"" + str.replace("\"", "\"\"") + "\"";
    }
    return str;
  }
}
