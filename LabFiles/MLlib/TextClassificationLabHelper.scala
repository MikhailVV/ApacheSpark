import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Row, SQLContext}


class TextClassificationLabHelper () {
  
      /**
      * Train a model on input data frame
      * Return the trained model 
      */
      def trainModel (trainDF: org.apache.spark.sql.DataFrame): org.apache.spark.ml.PipelineModel = {   
      
         // Processor for stage #1
         // Take text as input and produce words as tokens
         val text2wordsTokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

         // Processor for stage #2  
         // The number of features (10000, in our case) is an approximate length of our vocabulary
         // hashingTF algorithm receives its input as an output from the tokenizer (a list of words), stage #1) and produces words as features
         val hashingTF = new HashingTF().setNumFeatures(10000).setInputCol(text2wordsTokenizer.getOutputCol).setOutputCol("features")
         
         // Processor for stage #3
         // Set the maximum iteration number to converge (default is 100); more iterations yield higher accuracy at higher processing costs
         // Set the regularization parameter (default is 0) to control your model overfitting (sensitivity to data point variation) 
         // A high level of regularization parameter reduces overfitting but introduces bias to your estimate
         val logReg = new LogisticRegression().setMaxIter(10).setRegParam(0.01)
         
         // Configure a ML processing pipeline consisting of the above three stages
         val pipeline = new Pipeline().setStages(Array(text2wordsTokenizer, hashingTF, logReg))

         // Push the trainDF input file though the pipeline to train the model
         val model = pipeline.fit(trainDF)
         
         return model
      }


      /**
      *   Make predictions on test document(s) and print output
      */
      def predictor (testDF: org.apache.spark.sql.DataFrame, model: org.apache.spark.ml.PipelineModel): Unit = {
         // The input dataframe is modeled after a relational database table which contains named columns: id, text, etc.
         val predictor = model.transform(testDF).select("id", "text", "probability", "prediction").collect()
         
         // We map the results of the SQL-like select query to an instance of org.apache.spark.sql.Row
         predictor.foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) => println ("Doc ID: " + id + " Text: [" + text + "] Prob:" + prob + " Pred:" + prediction) }
   }
 }

