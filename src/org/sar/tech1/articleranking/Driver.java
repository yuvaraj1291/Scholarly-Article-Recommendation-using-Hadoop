package org.sar.tech1.articleranking;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Driver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration(true);

		args[0]="/home/cloudera/Downloads/input";
		args[1]="/home/cloudera/Downloads/Technique2/mr1_out";
		args[2]="/home/cloudera/Downloads/Technique2/mr2_out";
		int res = ToolRunner.run(conf, new InitialArticleScore(), args);
		if (res == 0) {
			args[0]=args[2];
			args[1]="/home/cloudera/Downloads/Technique2/mr3_out";

			res = ToolRunner.run( new PcComputation(), args);
			if (res == 0) {
				args[0]="/home/cloudera/Downloads/Technique2/mr2_out/";
				args[1]="/home/cloudera/Downloads/Technique2/mr4_out";

				res = ToolRunner.run(new ApComputation(), args);
				if (res == 0) {
					args[0]="/home/cloudera/Downloads/Technique2/mr2_out/";
					args[1]="/home/cloudera/Downloads/Technique2/mr5_out";

					res = ToolRunner.run(new VpComputation(), args);
					if (res == 0) {
						args[0]=args[1];
						args[1]="/home/cloudera/Downloads/Technique2/mr6_out";

						res = ToolRunner.run( new AvComputation(), args);
						if (res == 0) {
							args[0]=args[1]+"/*";
							args[1]="/home/cloudera/Downloads/Technique2/mr4_out/*";
							args[2]="/home/cloudera/Downloads/Technique2/mr7_out";

							res = ToolRunner.run(new ArComputation(), args);	
							if (res == 0) {
								args[0]=args[2]+"/*";
								args[1]="/home/cloudera/Downloads/Technique2/mr8_out";


								res = ToolRunner.run(new PaComputation(), args);	
								if (res == 0) {
									args[0]="/home/cloudera/Downloads/Technique2/mr5_out/*";
									args[1]="/home/cloudera/Downloads/Technique2/mr9_out";

									res = ToolRunner.run(new PvComputation(), args);	
									if (res == 0) {
										args[0]="/home/cloudera/Downloads/Technique2/mr2_out/*";
										args[1]="/home/cloudera/Downloads/Technique2/mr3_out/*";
										args[2]="/home/cloudera/Downloads/Technique2/mr8_out/*";
										args[3]="/home/cloudera/Downloads/Technique2/mr9_out/*";

										args[4]="/home/cloudera/Downloads/Technique2/mr10_out";

										res = ToolRunner.run(new ArticleScoreComputation(), args);	
										if (res == 0) {
											System.exit(res);
										}
									}
								}
							}

						}
					}
				}
			}
		}
	}
}