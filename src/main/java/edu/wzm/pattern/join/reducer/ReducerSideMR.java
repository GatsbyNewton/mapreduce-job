package edu.wzm.pattern.join.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/6/16.
 */
public class ReducerSideMR {

    public static class UserMapper extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            this.outKey.set(line[0]);
            this.outVal.set("A" + value.toString());
            context.write(this.outKey, this.outVal);
        }
    }

    public static class CommentMapper extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outVal = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().trim().split(",");
            this.outKey.set(line[0]);
            this.outVal.set("B" + value.toString());
            context.write(this.outKey, this.outVal);
        }
    }

    public static class ReducerSideReducer extends Reducer<Text, Text, Text, Text>{
        private List<Text> users = new ArrayList<Text>();
        private List<Text> comments = new ArrayList<Text>();
        private String joinType = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.joinType = context.getConfiguration().get("join.type");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            this.users.clear();
            this.comments.clear();

            // iterate through all our values, binning each record based on what
            // it was tagged with
            // make sure to remove the tag!
            for(Text val : values){
                String content = val.toString();
                if(content.charAt(0) == 'A'){
                    this.users.add(new Text(content.substring(1)));
                }
                else if(content.charAt(0) == 'B'){
                    this.comments.add(new Text(content.substring(1)));
                }
            }

            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context)throws IOException, InterruptedException{

            if(this.joinType.equalsIgnoreCase("inner")){
                // If both lists are not empty, join A with B.
                if(!this.users.isEmpty() && !this.comments.isEmpty()){
                    for(Text user : this.users){
                        for(Text comment : this.comments){
                            context.write(user, comment);
                        }
                    }
                }
            }
            else if(this.joinType.equalsIgnoreCase("left")){
                // For each entry in A,
                for(Text user : this.users){
                    // If list B is not empty, join A and B
                    if(!this.comments.isEmpty()){
                        for(Text comment : this.comments){
                            context.write(user, comment);
                        }
                    }
                    else {
                        // Else, output A by itself.
                        context.write(user, new Text(""));
                    }
                }
            }
            else if(this.joinType.equalsIgnoreCase("right")){
                // For each entry in B,
                for(Text commet : this.comments){
                    // If list A is not empty, join A and B
                    if(!this.users.isEmpty()){
                        for(Text user : this.users){
                            context.write(user, commet);
                        }
                    }
                    else {
                        context.write(new Text(""), commet);
                    }
                }
            }
            else if(this.joinType.equalsIgnoreCase("full")){
                // If list A is not empty
                if(!this.users.isEmpty()){
                    // For each entry in A
                    for(Text user : this.users){
                        // If list B is not empty, join A with B
                        if(!this.comments.isEmpty()){
                            for(Text comment : this.comments){
                                context.write(user, comment);
                            }
                        }
                        else {
                            // Else, output A by itself
                            context.write(user, new Text(""));
                        }
                    }
                }
                else {
                    // If list A is empty, just output B
                    for(Text comment : this.comments){
                        context.write(new Text(""), comment);
                    }
                }
            }
            else if(this.joinType.equalsIgnoreCase("anti")){
                // If list A is empty and B is empty or vice versa.
                if(this.users.isEmpty() ^ this.comments.isEmpty()){
                    // Iterate both A and B with null values
                    // The previous XOR check will make sure exactly one of
                    // these lists is empty and therefore won't have output
                    for(Text user : this.users){
                        context.write(user, new Text(""));
                    }

                    for(Text comment : this.comments){
                        context.write(new Text(""), comment);
                    }
                }
            }
            else {
                throw new RuntimeException(
                        "Join type not set to inner, leftouter, rightouter, fullouter, or anti");
            }
        }
    }
}
