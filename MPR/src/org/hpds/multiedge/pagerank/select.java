package org.hpds.multiedge.pagerank;

import org.apache.hadoop.util.ProgramDriver;

public class select {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("pager", pager.class, "Pagerank algorithm.");

			pgd.addClass("chgraph", chgraph.class, "Create dependency graph.");
			
			pgd.addClass("flownum", flownum.class, "PageRank result list");
			
			pgd.addClass("failcon", failedcon.class, "Filter failed connections.");
			
			pgd.addClass("feature", feature.class, "Feature processing.");
			
			pgd.addClass("cofeature", cofeature.class, "Combine feature list.");
			
			//pgd.addClass("p2plist", P2Plist.class, "List P2P logs.");
			
			pgd.addClass("ratio", failedcon_ratio.class, "List failed connections ratio.");
			
			//pgd.addClass("graph", graph.class, "Create dependence graph");
			
			//pgd.addClass("combine", combine.class, "Combine dependence graphs");
			
			pgd.addClass("graph", graph3_r.class, "Create dependence graph");
			
			pgd.addClass("inputtest", inputtest.class, "Create dependence graph");
			
			pgd.addClass("pagerank", pagerank.class, "Pagerank algorithm.");
			
			pgd.addClass("pagerank2", pagerank2.class, "Pagerank2 algorithm.");
			
			pgd.addClass("result", result.class, "PageRank result");
			
			pgd.addClass("resultlist", result_list.class, "PageRank result list");
			
			pgd.addClass("ipresult", ipresult.class, "PageRank result");
			
			pgd.addClass("valid", validation.class, "PageRank result");
			
			pgd.addClass("pageranked", pagerank_edge.class, "Pagerank algorithm.");

			pgd.driver(argv);

			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
