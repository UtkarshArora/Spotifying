import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AudioFeaturesByYear {

    /**
     * Mapper
     * Input: each line of Spotify_cleaned.csv
     * Output key: decade (e.g., 1960, 1970, ...)
     * Output value: tab-separated features + "1" (as count)
     *
     * Columns in Spotify_cleaned.csv:
     * 0: valence
     * 1: year
     * 2: acousticness
     * 3: artists
     * 4: danceability
     * 5: duration_ms
     * 6: energy
     * 7: explicit
     * 8: id
     * 9: instrumentalness
     * 10: key
     * 11: liveness
     * 12: loudness
     * 13: mode
     * 14: name
     * 15: popularity
     * 16: release_date
     * 17: speechiness
     * 18: tempo
     */
    public static class FeaturesMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        private boolean headerSkipped = false;
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            if (!headerSkipped) {
                if (line.toLowerCase().startsWith("valence")) {
                    headerSkipped = true;
                    return;
                } else {
                    headerSkipped = true;
                }
            }

            String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            if (fields.length < 19) {
                return;
            }

            try {
                double valence = Double.parseDouble(fields[0]);
                int year = Integer.parseInt(fields[1]);
                double acousticness = Double.parseDouble(fields[2]);
                double danceability = Double.parseDouble(fields[4]);
                double energy = Double.parseDouble(fields[6]);
                double instrumentalness = Double.parseDouble(fields[9]);
                double liveness = Double.parseDouble(fields[11]);
                double loudness = Double.parseDouble(fields[12]);
                double speechiness = Double.parseDouble(fields[17]);
                double tempo = Double.parseDouble(fields[18]);

                if (year < 1960 || year > 2020) {
                    return;
                }

                outKey.set(year);

                StringBuilder sb = new StringBuilder();
                sb.append(valence).append('\t')
                        .append(danceability).append('\t')
                        .append(energy).append('\t')
                        .append(acousticness).append('\t')
                        .append(instrumentalness).append('\t')
                        .append(liveness).append('\t')
                        .append(loudness).append('\t')
                        .append(speechiness).append('\t')
                        .append(tempo).append('\t');

                outValue.set(sb.toString());
                context.write(outKey, outValue);

            } catch (NumberFormatException e) {
            }
        }
    }

    public static class FeaturesReducer
            extends Reducer<IntWritable, Text, Text, NullWritable> {

        private Text out = new Text();
        private boolean headerWritten = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            if (!headerWritten) {
                out.set("year,avg_valence,avg_danceability,avg_energy,"
                        + "avg_acousticness,avg_instrumentalness,avg_liveness,"
                        + "avg_loudness,avg_speechiness,avg_tempo,song_count");
                context.write(out, NullWritable.get());
                headerWritten = true;
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sumValence = 0.0;
            double sumDanceability = 0.0;
            double sumEnergy = 0.0;
            double sumAcousticness = 0.0;
            double sumInstrumentalness = 0.0;
            double sumLiveness = 0.0;
            double sumLoudness = 0.0;
            double sumSpeechiness = 0.0;
            double sumTempo = 0.0;
            long totalCount = 0;

            for (Text t : values) {
                String[] parts = t.toString().split("\t");
                if (parts.length != 9) {
                    continue;
                }
                try {
                    double valence = Double.parseDouble(parts[0]);
                    double danceability = Double.parseDouble(parts[1]);
                    double energy = Double.parseDouble(parts[2]);
                    double acousticness = Double.parseDouble(parts[3]);
                    double instrumentalness = Double.parseDouble(parts[4]);
                    double liveness = Double.parseDouble(parts[5]);
                    double loudness = Double.parseDouble(parts[6]);
                    double speechiness = Double.parseDouble(parts[7]);
                    double tempo = Double.parseDouble(parts[8]);

                    sumValence += valence;
                    sumDanceability += danceability;
                    sumEnergy += energy;
                    sumAcousticness += acousticness;
                    sumInstrumentalness += instrumentalness;
                    sumLiveness += liveness;
                    sumLoudness += loudness;
                    sumSpeechiness += speechiness;
                    sumTempo += tempo;
                    totalCount++;
                } catch (NumberFormatException e) {
                }
            }

            if (totalCount == 0) {
                return;
            }

            double avgValence = sumValence / totalCount;
            double avgDanceability = sumDanceability / totalCount;
            double avgEnergy = sumEnergy / totalCount;
            double avgAcousticness = sumAcousticness / totalCount;
            double avgInstrumentalness = sumInstrumentalness / totalCount;
            double avgLiveness = sumLiveness / totalCount;
            double avgLoudness = sumLoudness / totalCount;
            double avgSpeechiness = sumSpeechiness / totalCount;
            double avgTempo = sumTempo / totalCount;

            int year = key.get();

            StringBuilder sb = new StringBuilder();
            sb.append(year).append(',')
                    .append(avgValence).append(',')
                    .append(avgDanceability).append(',')
                    .append(avgEnergy).append(',')
                    .append(avgAcousticness).append(',')
                    .append(avgInstrumentalness).append(',')
                    .append(avgLiveness).append(',')
                    .append(avgLoudness).append(',')
                    .append(avgSpeechiness).append(',')
                    .append(avgTempo).append(',')
                    .append(totalCount);

            out.set(sb.toString());
            context.write(out, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AudioFeaturesByYear <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Audio Features by Year (1960-2020)");

        job.setJarByClass(AudioFeaturesByYear.class);
        job.setMapperClass(FeaturesMapper.class);
        job.setReducerClass(FeaturesReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
