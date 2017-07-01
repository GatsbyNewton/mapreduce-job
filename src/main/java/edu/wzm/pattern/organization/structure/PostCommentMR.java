package edu.wzm.pattern.organization.structure;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/4/13.
 */
public class PostCommentMR {

    public static class PostMapper extends Mapper<Object, Text, Text, Text>{

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");
            outKey.set(line[0]);
            outValue.set("P" + line[1]);

            context.write(outKey, outValue);
        }
    }

    public static class CommentMapper extends Mapper<Object, Text, Text, Text>{

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            outKey.set(line[0]);
            outValue.set("C" + line[1]);

            context.write(outKey, outValue);
        }
    }

    public static class PostCommentReducer extends Reducer<Text, Text, Text, Text>{

        private String post = null;
        private List<String> comments = new ArrayList<String>();
        private Text outValue = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value : values){
                if(value.charAt(0) == 'P'){
                    post = value.toString();
                }
                else {
                    comments.add(value.toString());
                }
            }

            outValue.set(post + ": " + StringUtils.join(comments, ','));
            context.write(key, outValue);
        }
    }
}
