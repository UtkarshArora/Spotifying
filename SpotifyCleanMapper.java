import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpotifyCleanMapper
        extends Mapper<LongWritable, Text, NullWritable, Text> {

    private static final int NUM_COLUMNS = 19;

    private static final int IDX_VALENCE = 0;
    private static final int IDX_YEAR = 1;
    private static final int IDX_ACOUSTICNESS = 2;
    private static final int IDX_ARTISTS = 3;
    private static final int IDX_DANCEABILITY = 4;
    private static final int IDX_DURATION_MS = 5;
    private static final int IDX_ENERGY = 6;
    private static final int IDX_EXPLICIT = 7;
    private static final int IDX_ID = 8;
    private static final int IDX_INSTRUMENTAL = 9;
    private static final int IDX_KEY = 10;
    private static final int IDX_LIVENESS = 11;
    private static final int IDX_LOUDNESS = 12;
    private static final int IDX_MODE = 13;
    private static final int IDX_NAME = 14;
    private static final int IDX_POPULARITY = 15;
    private static final int IDX_RELEASE_DATE = 16;
    private static final int IDX_SPEECHINESS = 17;
    private static final int IDX_TEMPO = 18;

    private Text outValue = new Text();
    private boolean headerSeen = false;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (!headerSeen && line.startsWith("valence")) {
            headerSeen = true;
            outValue.set(line);
            context.write(NullWritable.get(), outValue);
            return;
        }

        if (line.trim().isEmpty()) {
            return;
        }

        String[] fields = line.split(",", -1);

        if (fields.length != NUM_COLUMNS) {
            context.getCounter("CLEANING", "BAD_NUM_FIELDS").increment(1);
            return;
        }

        fields[IDX_VALENCE] = fixDoubleFeature(
                fields[IDX_VALENCE],
                0.0, 1.0,
                0.5,
                "VALENCE_MISSING",
                "VALENCE_PARSE_ERR",
                "VALENCE_CLIPPED",
                context);

        fields[IDX_YEAR] = fixIntFeature(
                fields[IDX_YEAR],
                1921, 2020,
                2000,
                "YEAR_MISSING",
                "YEAR_PARSE_ERR",
                "YEAR_CLIPPED",
                context);

        fields[IDX_ACOUSTICNESS] = fixDoubleFeature(
                fields[IDX_ACOUSTICNESS],
                0.0, 1.0,
                0.5,
                "ACOUSTIC_MISSING",
                "ACOUSTIC_PARSE_ERR",
                "ACOUSTIC_CLIPPED",
                context);

        fields[IDX_ARTISTS] = fixArtists(fields[IDX_ARTISTS], context);

        fields[IDX_DANCEABILITY] = fixDoubleFeature(
                fields[IDX_DANCEABILITY],
                0.0, 0.99,
                0.5,
                "DANCE_MISSING",
                "DANCE_PARSE_ERR",
                "DANCE_CLIPPED",
                context);

        fields[IDX_DURATION_MS] = fixLongFeature(
                fields[IDX_DURATION_MS],
                5108L, 5_400_000L,
                180_000L,
                "DUR_MISSING",
                "DUR_PARSE_ERR",
                "DUR_CLIPPED",
                context);

        fields[IDX_ENERGY] = fixDoubleFeature(
                fields[IDX_ENERGY],
                0.0, 1.0,
                0.5,
                "ENERGY_MISSING",
                "ENERGY_PARSE_ERR",
                "ENERGY_CLIPPED",
                context);

        fields[IDX_EXPLICIT] = fixBinary01(
                fields[IDX_EXPLICIT],
                "EXPL_MISSING",
                "EXPL_PARSE_ERR",
                context);

        String fixedId = fixId(fields[IDX_ID], context);
        if (fixedId == null) {
            context.getCounter("CLEANING", "ID_MISSING_DROPPED").increment(1);
            return;
        }
        fields[IDX_ID] = fixedId;

        fields[IDX_INSTRUMENTAL] = fixDoubleFeature(
                fields[IDX_INSTRUMENTAL],
                0.0, 1.0,
                0.0,
                "INSTR_MISSING",
                "INSTR_PARSE_ERR",
                "INSTR_CLIPPED",
                context);

        fields[IDX_KEY] = fixKey(fields[IDX_KEY], context);

        fields[IDX_LIVENESS] = fixDoubleFeature(
                fields[IDX_LIVENESS],
                0.0, 1.0,
                0.5,
                "LIVENESS_MISSING",
                "LIVENESS_PARSE_ERR",
                "LIVENESS_CLIPPED",
                context);

        fields[IDX_LOUDNESS] = fixDoubleFeature(
                fields[IDX_LOUDNESS],
                -60.0, 3.85,
                -10.0,
                "LOUD_MISSING",
                "LOUD_PARSE_ERR",
                "LOUD_CLIPPED",
                context);

        fields[IDX_MODE] = fixBinary01(
                fields[IDX_MODE],
                "MODE_MISSING",
                "MODE_PARSE_ERR",
                context);

        fields[IDX_NAME] = fixName(fields[IDX_NAME], context);

        fields[IDX_POPULARITY] = fixIntFeature(
                fields[IDX_POPULARITY],
                0, 100,
                50,
                "POP_MISSING",
                "POP_PARSE_ERR",
                "POP_CLIPPED",
                context);

        String yearFromDate = extractYearStr(fields[IDX_RELEASE_DATE]);
        fields[IDX_RELEASE_DATE] = fixIntFeature(
                yearFromDate,
                1921, 2020,
                1970,
                "RELDATE_MISSING",
                "RELDATE_PARSE_ERR",
                "RELDATE_CLIPPED",
                context);

        fields[IDX_SPEECHINESS] = fixDoubleFeature(
                fields[IDX_SPEECHINESS],
                0.0, 0.97,
                0.48,
                "SPEECH_MISSING",
                "SPEECH_PARSE_ERR",
                "SPEECH_CLIPPED",
                context);

        fields[IDX_TEMPO] = fixDoubleFeature(
                fields[IDX_TEMPO],
                0.0, 244.0,
                120.0,
                "TEMPO_MISSING",
                "TEMPO_PARSE_ERR",
                "TEMPO_CLIPPED",
                context);

        String cleanedLine = String.join(",", fields);
        outValue.set(cleanedLine);
        context.write(NullWritable.get(), outValue);
    }

    private static boolean isMissing(String s) {
        return s == null || s.trim().isEmpty();
    }

    private String fixDoubleFeature(String s,
            double min, double max,
            double defaultVal,
            String missingCounter,
            String parseErrCounter,
            String clippedCounter,
            Context context) {
        if (isMissing(s)) {
            context.getCounter("CLEANING_FIX", missingCounter).increment(1);
            return Double.toString(defaultVal);
        }
        try {
            double v = Double.parseDouble(s.trim());
            if (v < min) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = min;
            } else if (v > max) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = max;
            }
            return Double.toString(v);
        } catch (NumberFormatException e) {
            context.getCounter("CLEANING_FIX", parseErrCounter).increment(1);
            return Double.toString(defaultVal);
        }
    }

    private String fixIntFeature(String s,
            int min, int max,
            int defaultVal,
            String missingCounter,
            String parseErrCounter,
            String clippedCounter,
            Context context) {
        if (isMissing(s)) {
            context.getCounter("CLEANING_FIX", missingCounter).increment(1);
            return Integer.toString(defaultVal);
        }
        try {
            int v = Integer.parseInt(s.trim());
            if (v < min) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = min;
            } else if (v > max) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = max;
            }
            return Integer.toString(v);
        } catch (NumberFormatException e) {
            context.getCounter("CLEANING_FIX", parseErrCounter).increment(1);
            return Integer.toString(defaultVal);
        }
    }

    private String fixLongFeature(String s,
            long min, long max,
            long defaultVal,
            String missingCounter,
            String parseErrCounter,
            String clippedCounter,
            Context context) {
        if (isMissing(s)) {
            context.getCounter("CLEANING_FIX", missingCounter).increment(1);
            return Long.toString(defaultVal);
        }
        try {
            long v = Long.parseLong(s.trim());
            if (v < min) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = min;
            } else if (v > max) {
                context.getCounter("CLEANING_FIX", clippedCounter).increment(1);
                v = max;
            }
            return Long.toString(v);
        } catch (NumberFormatException e) {
            context.getCounter("CLEANING_FIX", parseErrCounter).increment(1);
            return Long.toString(defaultVal);
        }
    }

    private String fixBinary01(String s,
            String missingCounter,
            String parseErrCounter,
            Context context) {
        if (isMissing(s)) {
            context.getCounter("CLEANING_FIX", missingCounter).increment(1);
            return "0";
        }
        String t = s.trim().toLowerCase();
        if (t.equals("0") || t.equals("1"))
            return t;
        if (t.equals("true") || t.equals("yes") || t.equals("y"))
            return "1";
        if (t.equals("false") || t.equals("no") || t.equals("n"))
            return "0";

        context.getCounter("CLEANING_FIX", parseErrCounter).increment(1);
        return "0";
    }

    private String fixId(String s, Context context) {
        if (s == null)
            s = "";
        String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    private String fixArtists(String s, Context context) {
        if (s == null)
            s = "";
        String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            context.getCounter("CLEANING_FIX", "ARTISTS_IMPUTED_UNKNOWN").increment(1);
            return "Unknown Artist";
        }
        return trimmed;
    }

    private String fixName(String s, Context context) {
        if (s == null)
            s = "";
        String trimmed = s.trim();
        if (trimmed.isEmpty()) {
            context.getCounter("CLEANING_FIX", "NAME_IMPUTED_UNKNOWN").increment(1);
            return "Unknown Title";
        }
        return trimmed;
    }

    private String fixKey(String s, Context context) {
        if (isMissing(s)) {
            context.getCounter("CLEANING_FIX", "KEY_MISSING").increment(1);
            return "0";
        }
        try {
            int v = Integer.parseInt(s.trim());
            if (v < 0) {
                context.getCounter("CLEANING_FIX", "KEY_NEGATIVE_CLIPPED").increment(1);
                v = 0;
            }
            if (v > 11) {
                context.getCounter("CLEANING_FIX", "KEY_MOD_12").increment(1);
                v = v % 12;
            }
            return Integer.toString(v);
        } catch (NumberFormatException e) {
            context.getCounter("CLEANING_FIX", "KEY_PARSE_ERR").increment(1);
            return "0";
        }
    }

    private String extractYearStr(String s) {
        if (s == null)
            return "";
        String t = s.trim();
        if (t.length() < 4)
            return "";
        return t.substring(0, 4);
    }
}
