package ua.karazin.parallelcomputing;

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

import java.io.IOException;


public class CountLines {

    // Клас для маппера, що перетворює вхідні рядки у пари ключ-значення.
    public static class LineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);  // Фіксоване значення 1 для кожного ключа
        private Text word = new Text();  // Об'єкт для зберігання ключового слова

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Перетворення вхідного значення в рядок
            String line = value.toString();
            System.out.println("Processing line: " + line);  // Виведення оброблюваної строки для дебагу

            // Якщо рядок містить певне ключове слово, записуємо його в контекст
            if (line.contains("Attribute")) {
                word.set("Attribute");
                context.write(word, one);
            } else if (line.contains("Case")) {
                word.set("Case");
                context.write(word, one);
            } else if (line.contains("Vote")) {
                word.set("Vote");
                context.write(word, one);
            }
        }
    }

    // Клас для редюсера, що підсумовує значення для кожного ключа.
    public static class LineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Підсумовуємо значення для кожного ключа
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Записуємо результат в контекст
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();  // Налаштування Hadoop
        Job job = Job.getInstance(conf, "Count Lines");  // Створення нового завдання
        job.setJarByClass(CountLines.class);  // Вказуємо головний клас
        job.setMapperClass(LineMapper.class);  // Вказуємо клас маппера
        job.setCombinerClass(LineReducer.class);  // Використовуємо редюсер як комбінатор
        job.setReducerClass(LineReducer.class);  // Вказуємо клас редюсера
        job.setOutputKeyClass(Text.class);  // Ключем виходу буде текст
        job.setOutputValueClass(IntWritable.class);  // Значенням виходу буде ціле число
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Вказуємо шлях до вхідних даних
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Вказуємо шлях до вихідних даних
        System.exit(job.waitForCompletion(true) ? 0 : 1);  // Запускаємо завдання і завершуємо програму
    }
}
