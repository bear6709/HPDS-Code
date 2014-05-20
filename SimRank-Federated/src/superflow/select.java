package superflow;

import org.apache.hadoop.util.ProgramDriver;

public class select {

	public static void main(String argv[]) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("SFilterMR", SFilterMR.class, "Filter out 192.168 and white list.");

			pgd.addClass("SFlowMR", SFlowMR.class, "Merge flows with same pair without considering port & protocol");
			
			pgd.addClass("SFlowGroupMR", SFlowGroupMR.class, "Superflow group");
			
			pgd.addClass("LabelMR", LabelMR.class, "Give number ID to a group");
			
			pgd.addClass("GraphMR", GraphMR.class, "For each pair of groups, construct edge with relationship.");
			
			pgd.addClass("SimRankMR2", SimRankMR2.class, "Compute similarity between each pair of groups");
			
			pgd.addClass("SortMR", SortMR.class, "Sort SimRank by score");
			
			pgd.driver(argv);

			exitCode = 0;
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
