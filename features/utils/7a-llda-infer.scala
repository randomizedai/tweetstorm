// http://nlp.stanford.edu/software/tmt/0.3/
 
// tells Scala where to find the TMT classes
import scalanlp.io._;
import scalanlp.stage._;
import scalanlp.stage.text._;
import scalanlp.text.tokenize._;
import scalanlp.pipes.Pipes.global._;
 
import edu.stanford.nlp.tmt.stage._;
import edu.stanford.nlp.tmt.model.lda._;
import edu.stanford.nlp.tmt.model.llda._;
 
if (args.length != 3) {
  System.err.println("Arguments: modelPath input.tsv output.tsv");
  System.err.println("  modelPath:  trained LLDA model");
  System.err.println("  input.tsv:  path to input file with two tab separated columns: id, words");
  System.err.println("  output.tsv: id followed by (word (label:prob)*)* for each word in each doc");
  System.exit(-1);
}
 
val modelPath = file(args(0));
val inputPath = file(args(1));
val outputPath = file(args(2));
 
val tokenizer = {
  SimpleEnglishTokenizer() ~>            // tokenize on space and punctuation
  CaseFolder() ~>                        // lowercase everything
  WordsAndNumbersOnlyFilter() ~>         // ignore non-words and non-numbers
  MinimumLengthFilter(3) ~>
  StopWordFilter("en")
 }

System.err.println("Loading model ...");
 
val lldaModel = LoadCVB0LabeledLDA(modelPath);
System.err.println("Loading model ...");
val model = lldaModel.asCVB0LDA;
System.err.println("Loading model ...");
val source = CSVFile(inputPath) ~> IDColumn(1);
System.err.println("Loading model ...");
val text = source ~> Column(3) ~> TokenizeWith(tokenizer);
System.err.println("Loading model ...");
 
val dataset = LDADataset(text,lldaModel.termIndex);
 
System.err.println("Generating output ...");
val perDocTopicDistributions =
  //InferCVB0DocumentTopicAssignments(lldaModel,dataset);
  InferCVB0LabeledLDADocumentTopicDistributions(lldaModel, dataset);
  //InferGibbsLabeledLDADocumentTopicDistributions(lldaModel, dataset);
System.err.println("Infer CVB ...");
//Works
CSVFile(outputPath).write(perDocTopicDistributions);

//val topTerms = QueryTopicUsage(lldaModel, dataset, perDocTopicDistributions);
//CSVFile(outputPath).write(topTerms);

println("Estimating per-doc per-word topic distributions");
val perDocWordTopicDistributions = EstimatePerWordTopicDistributions(
  lldaModel, dataset, perDocTopicDistributions);
CSVFile(outputPath+".per-word-topic-distributions.csv").write(perDocWordTopicDistributions)
